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

type bp04Fixture struct {
	Directory                   string
	RunnerConfigDirectory       string
	NetworkOwnerConfigDirectory string
	EMQXConfigDirectory         string
	NodeConfigDirectories       []string
	CredentialsPath             string
	ManagementToken             string
}

type bp04ClusterConfig struct {
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
	BootstrapMembers     []bp04BootstrapMember `json:"bootstrapMembers"`
	SeedAddresses        []string              `json:"seedAddresses"`
	PeerTLS              bp04PeerTLSConfig     `json:"peerTLS"`
	Management           bp04ManagementConfig  `json:"management"`
	MQTT                 bp04MQTTConfig        `json:"mqtt"`
}

type bp04BootstrapMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type bp04PeerTLSConfig struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type bp04ManagementConfig struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
	AuditDirectory        string `json:"auditDirectory"`
}

type bp04MQTTConfig struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
}

type bp04SecurityConfig struct {
	JWT        bp04JWTConfig  `json:"jwt"`
	Identities []bp04Identity `json:"identities"`
	ACL        []bp04ACLRule  `json:"acl"`
}

type bp04JWTConfig struct {
	Issuer          string   `json:"issuer"`
	Audience        string   `json:"audience"`
	Algorithms      []string `json:"algorithms"`
	JWKSURL         string   `json:"jwksUrl"`
	CacheTTL        string   `json:"cacheTtl"`
	RefreshCooldown string   `json:"refreshCooldown"`
	ClockSkew       string   `json:"clockSkew"`
	HTTPTimeout     string   `json:"httpTimeout"`
}

type bp04Identity struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type bp04ACLRule struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

type bp04RunnerCredentials struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	ManagementToken string `json:"managementToken"`
}

type bp04CA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
}

type bp04NodeCertificate struct {
	certificatePEM []byte
	privateKeyPEM  []byte
	fingerprint    string
}

func newBP04Fixture(profile Profile, runID string) (*bp04Fixture, error) {
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("validate BP04 fixture profile: %w", err)
	}
	if runID == "" {
		return nil, fmt.Errorf("BP04 fixture run ID is required")
	}
	directory, err := os.MkdirTemp("", "narumq-bp04-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary BP04 fixture directory: %w", err)
	}
	fixture := &bp04Fixture{
		Directory:                   directory,
		RunnerConfigDirectory:       filepath.Join(directory, "runner"),
		NetworkOwnerConfigDirectory: filepath.Join(directory, "network-owner"),
		EMQXConfigDirectory:         filepath.Join(directory, "emqx"),
		NodeConfigDirectories:       make([]string, bp04NodeCount),
	}
	cleanup := func(err error) (*bp04Fixture, error) {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return cleanup(fmt.Errorf("secure BP04 fixture directory: %w", err))
	}
	for _, path := range []string{fixture.RunnerConfigDirectory, fixture.NetworkOwnerConfigDirectory, fixture.EMQXConfigDirectory} {
		if err := os.Mkdir(path, 0o700); err != nil {
			return cleanup(fmt.Errorf("create BP04 fixture directory %q: %w", path, err))
		}
	}
	const emqxACL = "{allow, {username, \"jwt\"}, all, [\"bp04/#\", \"$share/bp04g/bp04/#\"]}.\n{deny, all}.\n"
	if err := writeBP04FixtureFile(filepath.Join(fixture.EMQXConfigDirectory, "acl.conf"), []byte(emqxACL)); err != nil {
		return cleanup(fmt.Errorf("write BP04 EMQX ACL: %w", err))
	}

	adminBytes := make([]byte, 32)
	if _, err := rand.Read(adminBytes); err != nil {
		return cleanup(fmt.Errorf("generate BP04 management token: %w", err))
	}
	fixture.ManagementToken = hex.EncodeToString(adminBytes)
	ca, err := newBP04CA()
	if err != nil {
		return cleanup(err)
	}
	jwksJSON, accessToken, err := newBP04JWTFixture()
	if err != nil {
		return cleanup(err)
	}
	certificates := make([]bp04NodeCertificate, bp04NodeCount)
	members := make([]bp04BootstrapMember, bp04NodeCount)
	for index := range certificates {
		nodeID := index + 1
		dnsName := fmt.Sprintf("node%d.bp04.test", nodeID)
		certificates[index], err = newBP04NodeCertificate(ca, dnsName)
		if err != nil {
			return cleanup(err)
		}
		members[index] = bp04BootstrapMember{
			MemberID: uint64(nodeID), Address: dnsName + ":7000", DNSSAN: dnsName,
			CertificateSHA256: certificates[index].fingerprint,
		}
	}
	security := bp04SecurityConfig{
		JWT: bp04JWTConfig{
			Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
			JWKSURL: "http://127.0.0.1:18080/jwks.json", CacheTTL: "5m",
			RefreshCooldown: "30s", ClockSkew: "30s", HTTPTimeout: "5s",
		},
		Identities: []bp04Identity{{
			Issuer: "https://issuer.example", Subject: "bp04-client", TenantID: bp04TenantID,
			CredentialID: "bp04-client-credential", AllowedClientIDs: bp04ClientIDs(profile), Roles: []string{"client"},
		}},
		ACL: []bp04ACLRule{
			{TenantID: bp04TenantID, Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "bp04/#"},
			{TenantID: bp04TenantID, Role: "client", Effect: "allow", Action: "publish", TopicFilter: "bp04/#"},
		},
	}
	securityJSON, err := json.Marshal(security)
	if err != nil {
		return cleanup(fmt.Errorf("encode BP04 MQTT security config: %w", err))
	}
	if err := writeBP04FixtureFile(filepath.Join(fixture.RunnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP04 runner JWKS: %w", err))
	}
	if err := writeBP04FixtureFile(filepath.Join(fixture.NetworkOwnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP04 network owner JWKS: %w", err))
	}

	caFile := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certificate.Raw})
	for index, certificate := range certificates {
		nodeNumber := index + 1
		dnsName := fmt.Sprintf("node%d.bp04.test", nodeNumber)
		configDir := filepath.Join(directory, fmt.Sprintf("node-%d", nodeNumber))
		if err := os.Mkdir(configDir, 0o700); err != nil {
			return cleanup(fmt.Errorf("create BP04 node %d config directory: %w", nodeNumber, err))
		}
		fixture.NodeConfigDirectories[index] = configDir
		for name, contents := range map[string][]byte{
			"ca.pem": caFile, "node.pem": certificate.certificatePEM,
			"node-key.pem": certificate.privateKeyPEM, "jwks.json": jwksJSON,
			"admin.token": []byte(fixture.ManagementToken + "\n"), "mqtt-security.json": securityJSON,
		} {
			if err := writeBP04FixtureFile(filepath.Join(configDir, name), contents); err != nil {
				return cleanup(fmt.Errorf("write BP04 node %d fixture %s: %w", nodeNumber, name, err))
			}
		}
		config := bp04ClusterConfig{
			Mode: "cluster", ClusterID: bp04ClusterID, NodeID: uint64(nodeNumber),
			Directory: "/var/lib/narumq/node", ListenAddress: "0.0.0.0:7000",
			AdvertisedAddress: dnsName + ":7000", LocalDNSName: dnsName,
			ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
			Role: "bootstrap", BootstrapMembers: members, SeedAddresses: []string{},
			PeerTLS:    bp04PeerTLSConfig{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"},
			Management: bp04ManagementConfig{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token", AuditDirectory: "/var/lib/narumq/audit"},
			MQTT:       bp04MQTTConfig{TCPListenAddress: "0.0.0.0:1883", TLSListenAddress: "", SecurityConfigFile: "/config/mqtt-security.json"},
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return cleanup(fmt.Errorf("encode BP04 node %d cluster config: %w", nodeNumber, err))
		}
		if err := writeBP04FixtureFile(filepath.Join(configDir, "node.json"), configJSON); err != nil {
			return cleanup(fmt.Errorf("write BP04 node %d cluster config: %w", nodeNumber, err))
		}
	}

	fixture.CredentialsPath = filepath.Join(fixture.RunnerConfigDirectory, "credentials.json")
	credentialsJSON, err := json.Marshal(bp04RunnerCredentials{Username: "jwt", Password: accessToken, ManagementToken: fixture.ManagementToken})
	if err != nil {
		return cleanup(fmt.Errorf("encode BP04 runner credentials: %w", err))
	}
	if err := writeBP04FixtureFile(fixture.CredentialsPath, credentialsJSON); err != nil {
		return cleanup(fmt.Errorf("write BP04 runner credentials: %w", err))
	}
	return fixture, nil
}

func (fixture *bp04Fixture) Close() error {
	if fixture == nil || fixture.Directory == "" {
		return nil
	}
	directory := fixture.Directory
	fixture.Directory = ""
	return os.RemoveAll(directory)
}

func writeBP04FixtureFile(path string, contents []byte) error {
	return os.WriteFile(path, contents, 0o600)
}

func newBP04CA() (bp04CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp04CA{}, fmt.Errorf("generate BP04 cluster CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp04CA{}, fmt.Errorf("generate BP04 cluster CA serial: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "NaruMQ BP04 CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return bp04CA{}, fmt.Errorf("create BP04 cluster CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp04CA{}, fmt.Errorf("parse BP04 cluster CA certificate: %w", err)
	}
	return bp04CA{certificate: certificate, privateKey: key}, nil
}

func newBP04NodeCertificate(ca bp04CA, dnsName string) (bp04NodeCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp04NodeCertificate{}, fmt.Errorf("generate BP04 node certificate key for %s: %w", dnsName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp04NodeCertificate{}, fmt.Errorf("generate BP04 node certificate serial for %s: %w", dnsName, err)
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
		return bp04NodeCertificate{}, fmt.Errorf("create BP04 node certificate for %s: %w", dnsName, err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp04NodeCertificate{}, fmt.Errorf("parse BP04 node certificate for %s: %w", dnsName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return bp04NodeCertificate{}, fmt.Errorf("marshal BP04 node key for %s: %w", dnsName, err)
	}
	fingerprint := sha256.Sum256(certificate.Raw)
	return bp04NodeCertificate{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		fingerprint:    hex.EncodeToString(fingerprint[:]),
	}, nil
}

func newBP04JWTFixture() ([]byte, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", fmt.Errorf("generate BP04 MQTT signing key: %w", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("import BP04 MQTT signing key: %w", err)
	}
	if err := signingKey.Set("kid", "bp04-key"); err != nil {
		return nil, "", fmt.Errorf("set BP04 MQTT signing key ID: %w", err)
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set BP04 MQTT signing algorithm: %w", err)
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		return nil, "", fmt.Errorf("derive BP04 MQTT verification key: %w", err)
	}
	if err := publicKey.Set("kid", "bp04-key"); err != nil {
		return nil, "", fmt.Errorf("set BP04 MQTT verification key ID: %w", err)
	}
	if err := publicKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set BP04 MQTT verification algorithm: %w", err)
	}
	keySet := jwk.NewSet()
	if err := keySet.AddKey(publicKey); err != nil {
		return nil, "", fmt.Errorf("add BP04 MQTT verification key to JWKS: %w", err)
	}
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		return nil, "", fmt.Errorf("encode BP04 MQTT JWKS: %w", err)
	}
	token := jwt.New()
	token.Options().Enable(jwt.FlattenAudience)
	for name, value := range map[string]any{
		"iss": "https://issuer.example", "aud": []string{"narumq"},
		"sub": "bp04-client", "exp": time.Now().Add(2 * time.Hour),
	} {
		if err := token.Set(name, value); err != nil {
			return nil, "", fmt.Errorf("set BP04 MQTT JWT claim %q: %w", name, err)
		}
	}
	signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), signingKey))
	if err != nil {
		return nil, "", fmt.Errorf("sign BP04 MQTT access token: %w", err)
	}
	return jwksJSON, string(signedToken), nil
}
