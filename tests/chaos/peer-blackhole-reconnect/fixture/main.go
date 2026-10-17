// Command fixture writes synthetic, short-lived certificates and cluster configs for the
// peer blackhole reconnect chaos scenario (ADR-0059). Usage: fixture <output-directory>.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

type member struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func serial() *big.Int {
	value, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	must(err)
	return value
}

func main() {
	out := os.Args[1]
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must(err)
	caTemplate := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "peer reconnect repro CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(12 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	must(err)
	caCert, err := x509.ParseCertificate(caDER)
	must(err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	tokenBytes := make([]byte, 32)
	_, err = rand.Read(tokenBytes)
	must(err)
	token := hex.EncodeToString(tokenBytes)

	members := make([]member, 3)
	certs := make([][2][]byte, 3)
	for index := range members {
		dnsName := fmt.Sprintf("node%d.peer.test", index+1)
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		must(err)
		template := &x509.Certificate{
			SerialNumber: serial(), Subject: pkix.Name{CommonName: dnsName},
			NotBefore: now.Add(-time.Minute), NotAfter: now.Add(12 * time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			DNSNames:    []string{dnsName}, BasicConstraintsValid: true,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		must(err)
		keyDER, err := x509.MarshalECPrivateKey(key)
		must(err)
		sum := sha256.Sum256(der)
		members[index] = member{MemberID: uint64(index + 1), Address: dnsName + ":7000", DNSSAN: dnsName, CertificateSHA256: hex.EncodeToString(sum[:])}
		certs[index] = [2][]byte{
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		}
	}
	security := map[string]any{
		"jwt": map[string]any{
			"issuer": "https://issuer.example", "audience": "narumq", "algorithms": []string{"RS256"},
			"jwksUrl": "http://127.0.0.1:18080/jwks.json", "cacheTtlMillis": 300000,
			"refreshCooldownMillis": 30000, "clockSkewMillis": 30000, "httpTimeoutMillis": 5000,
		},
		"identities": []map[string]any{{
			"issuer": "https://issuer.example", "subject": "repro", "tenantId": "repro",
			"credentialId": "repro", "allowedClientIds": []string{"repro"}, "roles": []string{"client"},
		}},
		"acl": []map[string]any{{"tenantId": "repro", "role": "client", "effect": "allow", "action": "publish", "topicFilter": "repro/#"}},
	}
	securityJSON, err := json.Marshal(security)
	must(err)
	for index := range members {
		dnsName := members[index].DNSSAN
		dir := filepath.Join(out, fmt.Sprintf("node-%d", index+1))
		must(os.MkdirAll(dir, 0o755))
		config := map[string]any{
			"mode": "cluster", "clusterId": "narumq-peer-repro", "nodeId": index + 1,
			"directory": "/var/lib/narumq/node", "listenAddress": "0.0.0.0:7000",
			"advertisedAddress": dnsName + ":7000", "localDnsName": dnsName,
			"clusterProtocolMajor": 2, "clusterProtocolMinor": 0, "stateSchemaMajor": 1, "stateSchemaMinor": 0,
			"role": "bootstrap", "bootstrapMembers": members, "seedAddresses": []string{},
			"peerTLS":    map[string]string{"certFile": "/config/node.pem", "keyFile": "/config/node-key.pem", "caFile": "/config/ca.pem"},
			"management": map[string]string{"listenAddress": "0.0.0.0:8080", "clusterAdminTokenFile": "/config/admin.token", "auditDirectory": "/var/lib/narumq/audit"},
			"mqtt":       map[string]string{"tcpListenAddress": "0.0.0.0:1883", "tlsListenAddress": "", "securityConfigFile": "/config/mqtt-security.json"},
		}
		configJSON, err := json.Marshal(config)
		must(err)
		for name, data := range map[string][]byte{
			"ca.pem": caPEM, "node.pem": certs[index][0], "node-key.pem": certs[index][1],
			"admin.token": []byte(token + "\n"), "mqtt-security.json": securityJSON, "node.json": configJSON,
		} {
			must(os.WriteFile(filepath.Join(dir, name), data, 0o644))
		}
	}
	must(os.WriteFile(filepath.Join(out, "admin.token"), []byte(token), 0o600))
}
