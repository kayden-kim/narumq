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

type drainFixture struct {
	Directory   string
	AdminToken  string
	AccessToken string
	Nodes       []drainNodeFixture
}

type drainNodeFixture struct {
	ID         string
	DNSName    string
	ConfigDir  string
	ConfigPath string
}

type drainClusterConfig struct {
	Mode                 string                `json:"mode"`
	ClusterID            string                `json:"clusterId"`
	NodeID               uint64                `json:"nodeId"`
	Directory            string                `json:"directory"`
	ListenAddress        string                `json:"listenAddress"`
	AdvertisedAddress    string                `json:"advertisedAddress"`
	LocalDNSName         string                `json:"localDnsName"`
	ClusterProtocolMajor uint32                `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32                `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32                `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32                `json:"stateSchemaMinor"`
	Role                 string                `json:"role"`
	BootstrapMembers     []drainClusterMember  `json:"bootstrapMembers"`
	SeedAddresses        []string              `json:"seedAddresses"`
	PeerTLS              drainPeerTLSConfig    `json:"peerTLS"`
	Management           drainManagementConfig `json:"management"`
	MQTT                 drainMQTTConfig       `json:"mqtt"`
}

type drainClusterMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type drainPeerTLSConfig struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type drainManagementConfig struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
}

type drainMQTTConfig struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
}

type drainMQTTSecurityConfig struct {
	JWT        drainMQTTJWTConfig        `json:"jwt"`
	Identities []drainMQTTIdentityConfig `json:"identities"`
	ACL        []drainMQTTACLConfig      `json:"acl"`
}

type drainMQTTJWTConfig struct {
	Issuer          string   `json:"issuer"`
	Audience        string   `json:"audience"`
	Algorithms      []string `json:"algorithms"`
	JWKSURL         string   `json:"jwksUrl"`
	CacheTTL        string   `json:"cacheTtl"`
	RefreshCooldown string   `json:"refreshCooldown"`
	ClockSkew       string   `json:"clockSkew"`
	HTTPTimeout     string   `json:"httpTimeout"`
}

type drainMQTTIdentityConfig struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type drainMQTTACLConfig struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

type drainCA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
	pem         []byte
}

type drainNodeCertificate struct {
	certificatePEM []byte
	privateKeyPEM  []byte
	fingerprint    string
}

func createDrainFixture() (*drainFixture, error) {
	directory, err := os.MkdirTemp("", "narumq-compat-drain-")
	if err != nil {
		return nil, fmt.Errorf("create temporary fixture directory: %w", err)
	}
	fixture := &drainFixture{Directory: directory}
	cleanup := func(err error) (*drainFixture, error) {
		_ = os.RemoveAll(directory)
		return nil, err
	}

	adminBytes := make([]byte, 32)
	if _, err := rand.Read(adminBytes); err != nil {
		return cleanup(fmt.Errorf("generate management token: %w", err))
	}
	fixture.AdminToken = hex.EncodeToString(adminBytes)

	ca, err := newDrainCA()
	if err != nil {
		return cleanup(err)
	}
	caPath := filepath.Join(directory, "ca.pem")
	if err := writeDrainFixtureFile(caPath, ca.pem); err != nil {
		return cleanup(fmt.Errorf("write cluster CA: %w", err))
	}

	jwksJSON, accessToken, err := newDrainJWTFixture()
	if err != nil {
		return cleanup(err)
	}
	fixture.AccessToken = accessToken

	certificates := make([]drainNodeCertificate, 3)
	for index := range certificates {
		nodeID := index + 1
		dnsName := fmt.Sprintf("node%d.compat.test", nodeID)
		certificates[index], err = newDrainNodeCertificate(ca, dnsName)
		if err != nil {
			return cleanup(err)
		}
	}

	members := make([]drainClusterMember, 3)
	for index, certificate := range certificates {
		nodeID := uint64(index + 1)
		dnsName := fmt.Sprintf("node%d.compat.test", nodeID)
		members[index] = drainClusterMember{
			MemberID: nodeID, Address: fmt.Sprintf("%s:7000", dnsName),
			DNSSAN: dnsName, CertificateSHA256: certificate.fingerprint,
		}
	}

	fixture.Nodes = make([]drainNodeFixture, 3)
	for index, certificate := range certificates {
		nodeNumber := index + 1
		nodeID := fmt.Sprint(nodeNumber)
		dnsName := fmt.Sprintf("node%d.compat.test", nodeNumber)
		configDir := filepath.Join(directory, "node-"+nodeID)
		if err := os.MkdirAll(configDir, 0o700); err != nil {
			return cleanup(fmt.Errorf("create node %s fixture directory: %w", nodeID, err))
		}
		for name, contents := range map[string][]byte{
			"ca.pem": ca.pem, "node.pem": certificate.certificatePEM,
			"node-key.pem": certificate.privateKeyPEM, "jwks.json": jwksJSON,
		} {
			if err := writeDrainFixtureFile(filepath.Join(configDir, name), contents); err != nil {
				return cleanup(fmt.Errorf("write node %s fixture %s: %w", nodeID, name, err))
			}
		}
		if err := writeDrainFixtureFile(filepath.Join(configDir, "admin.token"), []byte(fixture.AdminToken+"\n")); err != nil {
			return cleanup(fmt.Errorf("write node %s management token: %w", nodeID, err))
		}
		allowedClientIDs := []string{"compat-drain-client", "compat-drain-observer", "compat-drain-publisher"}
		security := drainMQTTSecurityConfig{
			JWT: drainMQTTJWTConfig{
				Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
				JWKSURL: "http://127.0.0.1:18080/jwks.json", CacheTTL: "5m",
				RefreshCooldown: "30s", ClockSkew: "30s", HTTPTimeout: "5s",
			},
			Identities: []drainMQTTIdentityConfig{{
				Issuer: "https://issuer.example", Subject: "compat-client", TenantID: "compat",
				CredentialID: "compat-client-credential", AllowedClientIDs: allowedClientIDs,
				Roles: []string{"client"},
			}},
			ACL: []drainMQTTACLConfig{
				{TenantID: "compat", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "compat/#"},
				{TenantID: "compat", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "compat/#"},
				{TenantID: "compat", Role: "client", Effect: "allow", Action: "will", TopicFilter: "compat/#"},
			},
		}
		securityJSON, err := json.Marshal(security)
		if err != nil {
			return cleanup(fmt.Errorf("encode node %s MQTT security config: %w", nodeID, err))
		}
		if err := writeDrainFixtureFile(filepath.Join(configDir, "mqtt-security.json"), securityJSON); err != nil {
			return cleanup(fmt.Errorf("write node %s MQTT security config: %w", nodeID, err))
		}

		config := drainClusterConfig{
			Mode: "cluster", ClusterID: "narumq-compat-drain", NodeID: uint64(nodeNumber),
			Directory: "/var/lib/narumq/node", ListenAddress: "0.0.0.0:7000",
			AdvertisedAddress: fmt.Sprintf("%s:7000", dnsName), LocalDNSName: dnsName,
			ClusterProtocolMajor: 2, ClusterProtocolMinor: 0,
			StateSchemaMajor: 1, StateSchemaMinor: 0, Role: "bootstrap",
			BootstrapMembers: members, SeedAddresses: []string{},
			PeerTLS:    drainPeerTLSConfig{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"},
			Management: drainManagementConfig{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token"},
			MQTT:       drainMQTTConfig{TCPListenAddress: "0.0.0.0:1883", TLSListenAddress: "", SecurityConfigFile: "/config/mqtt-security.json"},
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return cleanup(fmt.Errorf("encode node %s cluster config: %w", nodeID, err))
		}
		configPath := filepath.Join(configDir, "node.json")
		if err := writeDrainFixtureFile(configPath, configJSON); err != nil {
			return cleanup(fmt.Errorf("write node %s cluster config: %w", nodeID, err))
		}
		fixture.Nodes[index] = drainNodeFixture{ID: nodeID, DNSName: dnsName, ConfigDir: configDir, ConfigPath: "/config/node.json"}
	}
	clientDirectory := filepath.Join(directory, "client")
	if err := os.MkdirAll(clientDirectory, 0o700); err != nil {
		return cleanup(err)
	}
	credentials, err := json.Marshal(map[string]string{"username": "jwt", "password": accessToken})
	if err != nil {
		return cleanup(err)
	}
	if err := writeDrainFixtureFile(filepath.Join(clientDirectory, "credentials.json"), credentials); err != nil {
		return cleanup(err)
	}
	return fixture, nil
}

func (fixture *drainFixture) close() error {
	if fixture == nil || fixture.Directory == "" {
		return nil
	}
	return os.RemoveAll(fixture.Directory)
}

func writeDrainFixtureFile(path string, contents []byte) error {
	return os.WriteFile(path, contents, 0o600)
}

func newDrainCA() (drainCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return drainCA{}, fmt.Errorf("generate cluster CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return drainCA{}, fmt.Errorf("generate cluster CA serial: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "NaruMQ storage-drain CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return drainCA{}, fmt.Errorf("create cluster CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return drainCA{}, fmt.Errorf("parse cluster CA certificate: %w", err)
	}
	return drainCA{
		certificate: certificate, privateKey: key,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}, nil
}

func newDrainNodeCertificate(ca drainCA, dnsName string) (drainNodeCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return drainNodeCertificate{}, fmt.Errorf("generate node certificate key for %s: %w", dnsName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return drainNodeCertificate{}, fmt.Errorf("generate node certificate serial for %s: %w", dnsName, err)
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
		return drainNodeCertificate{}, fmt.Errorf("create node certificate for %s: %w", dnsName, err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return drainNodeCertificate{}, fmt.Errorf("parse node certificate for %s: %w", dnsName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return drainNodeCertificate{}, fmt.Errorf("marshal node key for %s: %w", dnsName, err)
	}
	fingerprint := sha256.Sum256(certificate.Raw)
	return drainNodeCertificate{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		fingerprint:    hex.EncodeToString(fingerprint[:]),
	}, nil
}

func newDrainJWTFixture() ([]byte, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", fmt.Errorf("generate MQTT signing key: %w", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("import MQTT signing key: %w", err)
	}
	if err := signingKey.Set("kid", "drain-key"); err != nil {
		return nil, "", fmt.Errorf("set MQTT signing key ID: %w", err)
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set MQTT signing algorithm: %w", err)
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		return nil, "", fmt.Errorf("derive MQTT verification key: %w", err)
	}
	if err := publicKey.Set("kid", "drain-key"); err != nil {
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
		"sub": "compat-client",
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
