package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
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

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

type chaosFixture struct {
	Directory   string
	AdminToken  string
	AccessToken string
	Nodes       []nodeFixture
}

type nodeFixture struct {
	ID         string
	DNSName    string
	ConfigDir  string
	ConfigPath string
}

type chaosClusterConfig struct {
	Mode                 string                `json:"mode"`
	ClusterID            string                `json:"clusterId"`
	NodeID               uint64                `json:"nodeId"`
	Directory            string                `json:"directory"`
	ListenAddress        string                `json:"listenAddress"`
	AdvertisedAddress    string                `json:"advertisedAddress"`
	LocalDNSName         string                `json:"localDnsName"`
	ReleaseVersion       string                `json:"releaseVersion"`
	ClusterProtocolMajor uint32                `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32                `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32                `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32                `json:"stateSchemaMinor"`
	Role                 string                `json:"role"`
	BootstrapMembers     []chaosClusterMember  `json:"bootstrapMembers"`
	SeedAddresses        []string              `json:"seedAddresses"`
	PeerTLS              chaosPeerTLSConfig    `json:"peerTLS"`
	Management           chaosManagementConfig `json:"management"`
	MQTT                 chaosMQTTConfig       `json:"mqtt"`
}

type chaosClusterMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type chaosPeerTLSConfig struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type chaosManagementConfig struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
}

type chaosMQTTConfig struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
}

type mqttSecurityConfig struct {
	JWT        mqttJWTConfig        `json:"jwt"`
	Identities []mqttIdentityConfig `json:"identities"`
	ACL        []mqttACLConfig      `json:"acl"`
}

type mqttJWTConfig struct {
	Issuer          string   `json:"issuer"`
	Audience        string   `json:"audience"`
	Algorithms      []string `json:"algorithms"`
	JWKSURL         string   `json:"jwksUrl"`
	CacheTTL        string   `json:"cacheTtl"`
	RefreshCooldown string   `json:"refreshCooldown"`
	ClockSkew       string   `json:"clockSkew"`
	HTTPTimeout     string   `json:"httpTimeout"`
}

type mqttIdentityConfig struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type mqttACLConfig struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

type chaosCA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
	pem         []byte
}

type chaosNodeCertificate struct {
	certificatePEM []byte
	privateKeyPEM  []byte
	fingerprint    string
}

func createChaosFixture() (*chaosFixture, error) {
	directory, err := os.MkdirTemp("", "narumq-chaos-")
	if err != nil {
		return nil, fmt.Errorf("create temporary fixture directory: %w", err)
	}
	fixture := &chaosFixture{Directory: directory}
	cleanup := func(err error) (*chaosFixture, error) {
		_ = os.RemoveAll(directory)
		return nil, err
	}

	adminBytes := make([]byte, 32)
	if _, err := rand.Read(adminBytes); err != nil {
		return cleanup(fmt.Errorf("generate management token: %w", err))
	}
	fixture.AdminToken = hex.EncodeToString(adminBytes)

	ca, err := newChaosCA()
	if err != nil {
		return cleanup(err)
	}
	caPath := filepath.Join(directory, "ca.pem")
	if err := writeFixtureFile(caPath, ca.pem); err != nil {
		return cleanup(fmt.Errorf("write cluster CA: %w", err))
	}

	jwksJSON, accessToken, err := newChaosJWTFixture()
	if err != nil {
		return cleanup(err)
	}
	fixture.AccessToken = accessToken

	certificates := make([]chaosNodeCertificate, chaosNodeCount)
	for index := range certificates {
		nodeID := index + 1
		dnsName := fmt.Sprintf("node%d.chaos.test", nodeID)
		certificates[index], err = newChaosNodeCertificate(ca, dnsName)
		if err != nil {
			return cleanup(err)
		}
	}

	members := make([]chaosClusterMember, chaosNodeCount)
	for index, certificate := range certificates {
		nodeID := uint64(index + 1)
		dnsName := fmt.Sprintf("node%d.chaos.test", nodeID)
		members[index] = chaosClusterMember{
			MemberID: nodeID, Address: fmt.Sprintf("%s:7000", dnsName),
			DNSSAN: dnsName, CertificateSHA256: certificate.fingerprint,
		}
	}

	fixture.Nodes = make([]nodeFixture, chaosNodeCount)
	for index, certificate := range certificates {
		nodeNumber := index + 1
		nodeID := fmt.Sprint(nodeNumber)
		dnsName := fmt.Sprintf("node%d.chaos.test", nodeNumber)
		configDir := filepath.Join(directory, "node-"+nodeID)
		if err := os.MkdirAll(configDir, 0o700); err != nil {
			return cleanup(fmt.Errorf("create node %s fixture directory: %w", nodeID, err))
		}
		for name, contents := range map[string][]byte{
			"ca.pem": ca.pem, "node.pem": certificate.certificatePEM,
			"node-key.pem": certificate.privateKeyPEM, "jwks.json": jwksJSON,
		} {
			if err := writeFixtureFile(filepath.Join(configDir, name), contents); err != nil {
				return cleanup(fmt.Errorf("write node %s fixture %s: %w", nodeID, name, err))
			}
		}
		if err := writeFixtureFile(filepath.Join(configDir, "admin.token"), []byte(fixture.AdminToken+"\n")); err != nil {
			return cleanup(fmt.Errorf("write node %s management token: %w", nodeID, err))
		}
		allowedClientIDs := []string{"chaos-baseline"}
		for probeID := 1; probeID <= 8; probeID++ {
			allowedClientIDs = append(allowedClientIDs, fmt.Sprintf("chaos-probe-%d", probeID))
		}
		for nodeID := 1; nodeID <= chaosNodeCount; nodeID++ {
			allowedClientIDs = append(allowedClientIDs, fmt.Sprintf("chaos-recovery-%d", nodeID))
		}
		allowedClientIDs = append(allowedClientIDs,
			"chaos-durable-sub", "chaos-durable-pub-1", "chaos-durable-pub-2", "chaos-durable-pub-3",
			"chaos-durable-halt-1", "chaos-durable-halt-2", "chaos-durable-sentinel")
		security := mqttSecurityConfig{
			JWT: mqttJWTConfig{
				Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
				JWKSURL: "http://127.0.0.1:18080/jwks.json", CacheTTL: "5m",
				RefreshCooldown: "30s", ClockSkew: "30s", HTTPTimeout: "5s",
			},
			Identities: []mqttIdentityConfig{{
				Issuer: "https://issuer.example", Subject: "chaos-client", TenantID: "chaos",
				CredentialID: "chaos-client-credential", AllowedClientIDs: allowedClientIDs,
				Roles: []string{"client"},
			}},
			ACL: []mqttACLConfig{
				{TenantID: "chaos", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "chaos/#"},
				{TenantID: "chaos", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "chaos/durable/#"},
			},
		}
		securityJSON, err := json.Marshal(security)
		if err != nil {
			return cleanup(fmt.Errorf("encode node %s MQTT security config: %w", nodeID, err))
		}
		if err := writeFixtureFile(filepath.Join(configDir, "mqtt-security.json"), securityJSON); err != nil {
			return cleanup(fmt.Errorf("write node %s MQTT security config: %w", nodeID, err))
		}

		config := chaosClusterConfig{
			Mode: "cluster", ClusterID: "narumq-chaos", NodeID: uint64(nodeNumber),
			Directory: "/var/lib/narumq/node", ListenAddress: "0.0.0.0:7000",
			AdvertisedAddress: fmt.Sprintf("%s:7000", dnsName), LocalDNSName: dnsName,
			ReleaseVersion: "1.0.0", ClusterProtocolMajor: 2, ClusterProtocolMinor: 0,
			StateSchemaMajor: 1, StateSchemaMinor: 0, Role: "bootstrap",
			BootstrapMembers: members, SeedAddresses: []string{},
			PeerTLS:    chaosPeerTLSConfig{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"},
			Management: chaosManagementConfig{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token"},
			MQTT:       chaosMQTTConfig{TCPListenAddress: "0.0.0.0:1883", TLSListenAddress: "", SecurityConfigFile: "/config/mqtt-security.json"},
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return cleanup(fmt.Errorf("encode node %s cluster config: %w", nodeID, err))
		}
		configPath := filepath.Join(configDir, "node.json")
		if err := writeFixtureFile(configPath, configJSON); err != nil {
			return cleanup(fmt.Errorf("write node %s cluster config: %w", nodeID, err))
		}
		fixture.Nodes[index] = nodeFixture{ID: nodeID, DNSName: dnsName, ConfigDir: configDir, ConfigPath: "/config/node.json"}
	}
	return fixture, nil
}

func (fixture *chaosFixture) close() error {
	if fixture == nil || fixture.Directory == "" {
		return nil
	}
	return os.RemoveAll(fixture.Directory)
}

func writeFixtureFile(path string, contents []byte) error {
	return os.WriteFile(path, contents, 0o600)
}

func newChaosCA() (chaosCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return chaosCA{}, fmt.Errorf("generate cluster CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return chaosCA{}, fmt.Errorf("generate cluster CA serial: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "NaruMQ storage-chaos CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return chaosCA{}, fmt.Errorf("create cluster CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return chaosCA{}, fmt.Errorf("parse cluster CA certificate: %w", err)
	}
	return chaosCA{
		certificate: certificate, privateKey: key,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}, nil
}

func newChaosNodeCertificate(ca chaosCA, dnsName string) (chaosNodeCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return chaosNodeCertificate{}, fmt.Errorf("generate node certificate key for %s: %w", dnsName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return chaosNodeCertificate{}, fmt.Errorf("generate node certificate serial for %s: %w", dnsName, err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: dnsName},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{dnsName, "localhost"}, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.privateKey)
	if err != nil {
		return chaosNodeCertificate{}, fmt.Errorf("create node certificate for %s: %w", dnsName, err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return chaosNodeCertificate{}, fmt.Errorf("parse node certificate for %s: %w", dnsName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return chaosNodeCertificate{}, fmt.Errorf("marshal node key for %s: %w", dnsName, err)
	}
	fingerprint := sha256.Sum256(certificate.Raw)
	return chaosNodeCertificate{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		fingerprint:    hex.EncodeToString(fingerprint[:]),
	}, nil
}

func newChaosJWTFixture() ([]byte, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", fmt.Errorf("generate MQTT signing key: %w", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("import MQTT signing key: %w", err)
	}
	if err := signingKey.Set("kid", "chaos-key"); err != nil {
		return nil, "", fmt.Errorf("set MQTT signing key ID: %w", err)
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set MQTT signing algorithm: %w", err)
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		return nil, "", fmt.Errorf("derive MQTT verification key: %w", err)
	}
	if err := publicKey.Set("kid", "chaos-key"); err != nil {
		return nil, "", fmt.Errorf("set MQTT verification key ID: %w", err)
	}
	if err := publicKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set MQTT verification algorithm: %w", err)
	}
	keySet := jwk.NewSet()
	if err := keySet.AddKey(publicKey); err != nil {
		return nil, "", fmt.Errorf("add MQTT verification key to JWKS: %w", err)
	}
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		return nil, "", fmt.Errorf("encode MQTT JWKS: %w", err)
	}
	token := jwt.New()
	for name, value := range map[string]any{
		"iss": "https://issuer.example",
		"aud": []string{"narumq"},
		"sub": "chaos-client",
		"exp": time.Now().Add(time.Hour),
	} {
		if err := token.Set(name, value); err != nil {
			return nil, "", fmt.Errorf("set MQTT JWT claim %q: %w", name, err)
		}
	}
	signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), signingKey))
	if err != nil {
		return nil, "", fmt.Errorf("sign MQTT access token: %w", err)
	}
	return jwksJSON, string(signedToken), nil
}
