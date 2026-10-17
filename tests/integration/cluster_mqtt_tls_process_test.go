package integration

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Traceability: SEC-03, SEC-04; docs/specs/configuration-and-runtime-options.md §6; ADR-0052.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-22, tenancy-and-security.md:SEC-SCN-004, tenancy-and-security.md:SEC-SCN-005, tenancy-and-security.md:SEC-SCN-006, tenancy-and-security.md:SEC-SCN-008
func TestDynamicClusterMQTTTLSClientCertificateProcessAcceptance(t *testing.T) {
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	mqttCA := newClusterMembershipProcessCA(t)
	server := newClusterMembershipProcessCertificate(t, mqttCA, "mqtt.example.test")
	device := newClusterMembershipProcessCertificate(t, mqttCA, "cluster-mtls-device.example.test")
	unmapped := newClusterMembershipProcessCertificate(t, mqttCA, "cluster-mtls-unmapped.example.test")

	mqttCAPath := filepath.Join(fixture.tempDir, "certificates", "mqtt-ca.pem")
	serverCertPath := server.certPath(fixture.tempDir)
	serverKeyPath := server.keyPath(fixture.tempDir)
	clusterMembershipProcessWriteFile(t, mqttCAPath, mqttCA.pem, 0o600)
	clusterMembershipProcessWriteFile(t, serverCertPath, server.certPEM, 0o600)
	clusterMembershipProcessWriteFile(t, serverKeyPath, server.keyPEM, 0o600)

	sec07WriteSecurityConfig(t, fixture, map[string]any{
		"mtlsIdentities": []map[string]any{{
			"identityKind": "commonName", "identity": "cluster-mtls-device.example.test",
			"tenantId": "mtls", "subject": "cluster-mtls-device",
			"credentialId": "cluster-mtls-device-cert", "allowedClientIds": []string{"cluster-mtls-device"},
			"roles": []string{"client"},
		}},
		"acl": []any{},
	})

	tlsAddresses := clusterMembershipProcessAddresses(t, 3)
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		configPath := fixture.bootstrapConfig(uint64(index + 1))
		data, err := os.ReadFile(configPath)
		if err != nil {
			t.Fatalf("read node %d bootstrap config: %v", index+1, err)
		}
		var config clusterMembershipProcessConfig
		if err := json.Unmarshal(data, &config); err != nil {
			t.Fatalf("decode node %d bootstrap config: %v", index+1, err)
		}
		config.MQTT.TLSListenAddress = tlsAddresses[index]
		config.MQTT.TLS = &clusterMembershipProcessMQTTTLS{
			CertFile: serverCertPath, KeyFile: serverKeyPath, CAFile: mqttCAPath,
			ClientAuth: "require", MinVersion: "1.3",
		}
		clusterMembershipProcessWriteJSON(t, configPath, config)
		process, err := fixture.harness.start(
			fmt.Sprintf("Cluster MQTT TLS node %d", index+1), configPath, fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start Cluster MQTT TLS node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)
	for _, process := range processes {
		clusterMembershipProcessWaitForStatus(t, fixture.ctx, process, "/health/ready", http.StatusOK)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(mqttCA.pem) {
		t.Fatal("invalid MQTT CA certificate")
	}
	devicePair, err := tls.X509KeyPair(device.certPEM, device.keyPEM)
	if err != nil {
		t.Fatalf("load device client certificate: %v", err)
	}
	unmappedPair, err := tls.X509KeyPair(unmapped.certPEM, unmapped.keyPEM)
	if err != nil {
		t.Fatalf("load unmapped client certificate: %v", err)
	}
	clientTLSConfig := func(certificates ...tls.Certificate) *tls.Config {
		return &tls.Config{
			RootCAs: roots, ServerName: "mqtt.example.test", Certificates: certificates,
		}
	}

	for _, version := range []struct {
		name     string
		protocol byte
		wantAuth byte
	}{
		{name: "MQTT 3.1.1", protocol: 4, wantAuth: 0x05},
		{name: "MQTT 5", protocol: 5, wantAuth: 0x87},
	} {
		t.Run(version.name, func(t *testing.T) {
			for index, address := range tlsAddresses {
				state, connack, err := clusterMQTTTLSProcessConnect(address, clientTLSConfig(devicePair), "cluster-mtls-device", version.protocol)
				if err != nil || connack != 0x00 {
					t.Fatalf("node %d mTLS CONNECT CONNACK=0x%02x err=%v, want success", index+1, connack, err)
				}
				if state.Version != tls.VersionTLS13 {
					t.Fatalf("node %d TLS version = 0x%04x, want TLS 1.3", index+1, state.Version)
				}
				if len(state.PeerCertificates) == 0 {
					t.Fatalf("node %d TLS state has no server certificate", index+1)
				}
				serverFingerprint := sha256.Sum256(state.PeerCertificates[0].Raw)
				if serverFingerprint != server.fingerprint {
					t.Fatalf("node %d MQTT server certificate fingerprint = %x, want %x", index+1, serverFingerprint, server.fingerprint)
				}
				if serverFingerprint == fixture.certificates[index].fingerprint {
					t.Fatalf("node %d MQTT server certificate reused the peer certificate", index+1)
				}
				t.Logf("node %d mTLS CONNECT CONNACK=0x00 TLS=1.3 MQTT certificate differs from peer certificate", index+1)
			}

			_, connack, err := clusterMQTTTLSProcessConnect(tlsAddresses[0], clientTLSConfig(unmappedPair), "cluster-mtls-device", version.protocol)
			if err != nil || connack != version.wantAuth {
				t.Fatalf("unmapped certificate CONNACK=0x%02x err=%v, want MQTT %s 0x%02x", connack, err, version.name, version.wantAuth)
			}
			t.Logf("unmapped client certificate rejected with CONNACK=0x%02x", connack)
			_, connack, err = clusterMQTTTLSProcessConnect(tlsAddresses[0], clientTLSConfig(devicePair), "cluster-mtls-other", version.protocol)
			if err != nil || connack != version.wantAuth {
				t.Fatalf("unbound Client ID CONNACK=0x%02x err=%v, want MQTT %s 0x%02x", connack, err, version.name, version.wantAuth)
			}
			t.Logf("unbound Client ID rejected with CONNACK=0x%02x", connack)
		})
	}

	if _, _, err := clusterMQTTTLSProcessConnect(tlsAddresses[0], clientTLSConfig(), "cluster-mtls-device", 4); err == nil {
		t.Fatal("TLS connection without a client certificate succeeded")
	} else {
		t.Logf("client without a certificate rejected: %v", err)
	}
	tls12Config := clientTLSConfig(devicePair)
	tls12Config.MaxVersion = tls.VersionTLS12
	if _, _, err := clusterMQTTTLSProcessConnect(tlsAddresses[0], tls12Config, "cluster-mtls-device", 4); err == nil {
		t.Fatal("TLS 1.2 connection succeeded against a TLS 1.3 minimum")
	} else {
		t.Logf("TLS 1.2 client rejected: %v", err)
	}
}

func clusterMQTTTLSProcessConnect(address string, config *tls.Config, clientID string, protocolVersion byte) (tls.ConnectionState, byte, error) {
	if protocolVersion != 4 && protocolVersion != 5 {
		return tls.ConnectionState{}, 0, fmt.Errorf("unsupported MQTT protocol version %d", protocolVersion)
	}

	connection, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", address, config)
	if err != nil {
		return tls.ConnectionState{}, 0, err
	}
	defer connection.Close()
	if err := connection.Handshake(); err != nil {
		return tls.ConnectionState{}, 0, err
	}
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return tls.ConnectionState{}, 0, err
	}
	state := connection.ConnectionState()
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', protocolVersion, 0x02, 0, 60}
	if protocolVersion == 5 {
		body = append(body, 0)
	}
	body = append(body, byte(len(clientID)>>8), byte(len(clientID)))
	body = append(body, clientID...)
	packet := clusterMembershipProcessEncodeMQTTPacket(0x10, body)
	written, err := connection.Write(packet)
	if err != nil {
		return tls.ConnectionState{}, 0, err
	}
	if written != len(packet) {
		return tls.ConnectionState{}, 0, io.ErrShortWrite
	}
	header, response, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil {
		return tls.ConnectionState{}, 0, err
	}
	if header != 0x20 {
		return tls.ConnectionState{}, 0, fmt.Errorf("unexpected CONNACK header 0x%02x", header)
	}
	minimumLength := 2
	if protocolVersion == 5 {
		minimumLength = 3
	}
	if len(response) < minimumLength {
		return tls.ConnectionState{}, 0, fmt.Errorf("CONNACK body length = %d, want at least %d", len(response), minimumLength)
	}
	if protocolVersion == 5 && int(response[2]) != len(response)-3 {
		return tls.ConnectionState{}, 0, fmt.Errorf("MQTT 5 CONNACK properties length = %d, body length = %d", response[2], len(response))
	}
	return state, response[1], nil
}
