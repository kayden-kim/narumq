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

const bp07NodeCount = 3

type bp07Fixture struct {
	Directory                   string
	RunnerConfigDirectory       string
	NetworkOwnerConfigDirectory string
	EMQXConfigDirectory         string
	NodeConfigDirectories       []string
	CredentialsPath             string
	ManagementToken             string
}

type bp07ClusterConfig struct {
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
	BootstrapMembers     []bp07BootstrapMember `json:"bootstrapMembers"`
	SeedAddresses        []string              `json:"seedAddresses"`
	PeerTLS              bp07PeerTLSConfig     `json:"peerTLS"`
	Management           bp07ManagementConfig  `json:"management"`
	MQTT                 bp07MQTTConfig        `json:"mqtt"`
}

type bp07BootstrapMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type bp07PeerTLSConfig struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type bp07ManagementConfig struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
	AuditDirectory        string `json:"auditDirectory"`
}

type bp07MQTTConfig struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
}

type bp07SecurityConfig struct {
	JWT        bp07JWTConfig  `json:"jwt"`
	Identities []bp07Identity `json:"identities"`
	ACL        []bp07ACLRule  `json:"acl"`
}

type bp07JWTConfig struct {
	Issuer          string   `json:"issuer"`
	Audience        string   `json:"audience"`
	Algorithms      []string `json:"algorithms"`
	JWKSURL         string   `json:"jwksUrl"`
	CacheTTL        string   `json:"cacheTtl"`
	RefreshCooldown string   `json:"refreshCooldown"`
	ClockSkew       string   `json:"clockSkew"`
	HTTPTimeout     string   `json:"httpTimeout"`
}

type bp07Identity struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type bp07ACLRule struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

type bp07RunnerCredentials struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	ManagementToken string `json:"managementToken"`
}

type bp07CA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
	pem         []byte
}

type bp07NodeCertificate struct {
	certificatePEM []byte
	privateKeyPEM  []byte
	fingerprint    string
}

func newBP07Fixture(profile Profile, runID string) (*bp07Fixture, error) {
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("validate BP07 fixture profile: %w", err)
	}
	if runID == "" {
		return nil, fmt.Errorf("BP07 fixture run ID is required")
	}
	directory, err := os.MkdirTemp("", "narumq-bp07-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary BP07 fixture directory: %w", err)
	}
	fixture := &bp07Fixture{
		Directory:                   directory,
		RunnerConfigDirectory:       filepath.Join(directory, "runner"),
		NetworkOwnerConfigDirectory: filepath.Join(directory, "network-owner"),
		EMQXConfigDirectory:         filepath.Join(directory, "emqx"),
		NodeConfigDirectories:       make([]string, bp07NodeCount),
	}
	cleanup := func(err error) (*bp07Fixture, error) {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return cleanup(fmt.Errorf("secure BP07 fixture directory: %w", err))
	}
	if err := os.Mkdir(fixture.RunnerConfigDirectory, 0o700); err != nil {
		return cleanup(fmt.Errorf("create BP07 runner config directory: %w", err))
	}
	if err := os.Mkdir(fixture.NetworkOwnerConfigDirectory, 0o700); err != nil {
		return cleanup(fmt.Errorf("create BP07 network owner public config directory: %w", err))
	}
	if err := os.Mkdir(fixture.EMQXConfigDirectory, 0o700); err != nil {
		return cleanup(fmt.Errorf("create BP07 EMQX config directory: %w", err))
	}
	const emqxACL = "{allow, {username, \"jwt\"}, all, [\"bp07/#\"]}.\n{deny, all}.\n"
	if err := writeBP07FixtureFile(filepath.Join(fixture.EMQXConfigDirectory, "acl.conf"), []byte(emqxACL)); err != nil {
		return cleanup(fmt.Errorf("write BP07 EMQX ACL: %w", err))
	}

	adminBytes := make([]byte, 32)
	if _, err := rand.Read(adminBytes); err != nil {
		return cleanup(fmt.Errorf("generate BP07 management token: %w", err))
	}
	fixture.ManagementToken = hex.EncodeToString(adminBytes)

	ca, err := newBP07CA()
	if err != nil {
		return cleanup(err)
	}
	jwksJSON, accessToken, err := newBP07JWTFixture()
	if err != nil {
		return cleanup(err)
	}
	certificates := make([]bp07NodeCertificate, bp07NodeCount)
	members := make([]bp07BootstrapMember, bp07NodeCount)
	for index := range certificates {
		nodeID := index + 1
		dnsName := fmt.Sprintf("node%d.bp07.test", nodeID)
		certificates[index], err = newBP07NodeCertificate(ca, dnsName)
		if err != nil {
			return cleanup(err)
		}
		members[index] = bp07BootstrapMember{
			MemberID: uint64(nodeID), Address: dnsName + ":7000", DNSSAN: dnsName,
			CertificateSHA256: certificates[index].fingerprint,
		}
	}

	var allowedClientIDs []string
	for publisherNode := 1; publisherNode <= bp07NodeCount; publisherNode++ {
		for subscriberNode := 1; subscriberNode <= bp07NodeCount; subscriberNode++ {
			if publisherNode == subscriberNode {
				continue
			}
			pair := fmt.Sprintf("%d-%d", publisherNode, subscriberNode)
			allowedClientIDs = append(allowedClientIDs, "bp07-probe-pub-"+pair, "bp07-probe-sub-"+pair)
		}
	}
	for _, stream := range []string{"v5-q0", "v5-q1", "v5-q2", "v311-q1"} {
		allowedClientIDs = append(allowedClientIDs, "bp07-pub-"+stream, "bp07-sub-"+stream)
	}
	allowedClientIDs = append(allowedClientIDs, "bp07-will-v5-safe", "bp07-will-v311", "bp07-will-observer")
	for i := 1; i <= profile.IdleClients; i++ {
		allowedClientIDs = append(allowedClientIDs, fmt.Sprintf("bp07-idle-%02d", i))
	}
	security := bp07SecurityConfig{
		JWT: bp07JWTConfig{
			Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
			JWKSURL: "http://127.0.0.1:18080/jwks.json", CacheTTL: "5m",
			RefreshCooldown: "30s", ClockSkew: "30s", HTTPTimeout: "5s",
		},
		Identities: []bp07Identity{{
			Issuer: "https://issuer.example", Subject: "bp07-client", TenantID: "bp07",
			CredentialID: "bp07-client-credential", AllowedClientIDs: allowedClientIDs, Roles: []string{"client"},
		}},
		ACL: []bp07ACLRule{
			{TenantID: "bp07", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "bp07/#"},
			{TenantID: "bp07", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "bp07/#"},
			{TenantID: "bp07", Role: "client", Effect: "allow", Action: "will", TopicFilter: "bp07/#"},
		},
	}
	securityJSON, err := json.Marshal(security)
	if err != nil {
		return cleanup(fmt.Errorf("encode BP07 MQTT security config: %w", err))
	}
	if err := writeBP07FixtureFile(filepath.Join(fixture.RunnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP07 runner JWKS: %w", err))
	}
	if err := writeBP07FixtureFile(filepath.Join(fixture.NetworkOwnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP07 network owner JWKS: %w", err))
	}

	caFile := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certificate.Raw})
	for index, certificate := range certificates {
		nodeNumber := index + 1
		dnsName := fmt.Sprintf("node%d.bp07.test", nodeNumber)
		configDir := filepath.Join(directory, fmt.Sprintf("node-%d", nodeNumber))
		if err := os.Mkdir(configDir, 0o700); err != nil {
			return cleanup(fmt.Errorf("create BP07 node %d config directory: %w", nodeNumber, err))
		}
		fixture.NodeConfigDirectories[index] = configDir
		for name, contents := range map[string][]byte{
			"ca.pem": caFile, "node.pem": certificate.certificatePEM,
			"node-key.pem": certificate.privateKeyPEM, "jwks.json": jwksJSON,
			"admin.token": []byte(fixture.ManagementToken + "\n"), "mqtt-security.json": securityJSON,
		} {
			if err := writeBP07FixtureFile(filepath.Join(configDir, name), contents); err != nil {
				return cleanup(fmt.Errorf("write BP07 node %d fixture %s: %w", nodeNumber, name, err))
			}
		}
		config := bp07ClusterConfig{
			Mode: "cluster", ClusterID: "narumq-bp07", NodeID: uint64(nodeNumber),
			Directory: "/var/lib/narumq/node", ListenAddress: "0.0.0.0:7000",
			AdvertisedAddress: dnsName + ":7000", LocalDNSName: dnsName,
			ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
			Role: "bootstrap", BootstrapMembers: members, SeedAddresses: []string{},
			PeerTLS:    bp07PeerTLSConfig{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"},
			Management: bp07ManagementConfig{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token", AuditDirectory: "/var/lib/narumq/audit"},
			MQTT:       bp07MQTTConfig{TCPListenAddress: "0.0.0.0:1883", TLSListenAddress: "", SecurityConfigFile: "/config/mqtt-security.json"},
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return cleanup(fmt.Errorf("encode BP07 node %d cluster config: %w", nodeNumber, err))
		}
		if err := writeBP07FixtureFile(filepath.Join(configDir, "node.json"), configJSON); err != nil {
			return cleanup(fmt.Errorf("write BP07 node %d cluster config: %w", nodeNumber, err))
		}
	}

	fixture.CredentialsPath = filepath.Join(fixture.RunnerConfigDirectory, "credentials.json")
	credentialsJSON, err := json.Marshal(bp07RunnerCredentials{Username: "jwt", Password: accessToken, ManagementToken: fixture.ManagementToken})
	if err != nil {
		return cleanup(fmt.Errorf("encode BP07 runner credentials: %w", err))
	}
	if err := writeBP07FixtureFile(fixture.CredentialsPath, credentialsJSON); err != nil {
		return cleanup(fmt.Errorf("write BP07 runner credentials: %w", err))
	}
	return fixture, nil
}

func (fixture *bp07Fixture) Close() error {
	if fixture == nil || fixture.Directory == "" {
		return nil
	}
	directory := fixture.Directory
	fixture.Directory = ""
	return os.RemoveAll(directory)
}

func writeBP07FixtureFile(path string, contents []byte) error {
	return os.WriteFile(path, contents, 0o600)
}

func newBP07CA() (bp07CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp07CA{}, fmt.Errorf("generate BP07 cluster CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp07CA{}, fmt.Errorf("generate BP07 cluster CA serial: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "NaruMQ BP07 CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return bp07CA{}, fmt.Errorf("create BP07 cluster CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp07CA{}, fmt.Errorf("parse BP07 cluster CA certificate: %w", err)
	}
	return bp07CA{certificate: certificate, privateKey: key}, nil
}

func newBP07NodeCertificate(ca bp07CA, dnsName string) (bp07NodeCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp07NodeCertificate{}, fmt.Errorf("generate BP07 node certificate key for %s: %w", dnsName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp07NodeCertificate{}, fmt.Errorf("generate BP07 node certificate serial for %s: %w", dnsName, err)
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
		return bp07NodeCertificate{}, fmt.Errorf("create BP07 node certificate for %s: %w", dnsName, err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp07NodeCertificate{}, fmt.Errorf("parse BP07 node certificate for %s: %w", dnsName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return bp07NodeCertificate{}, fmt.Errorf("marshal BP07 node key for %s: %w", dnsName, err)
	}
	fingerprint := sha256.Sum256(certificate.Raw)
	return bp07NodeCertificate{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		fingerprint:    hex.EncodeToString(fingerprint[:]),
	}, nil
}

func newBP07JWTFixture() ([]byte, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", fmt.Errorf("generate BP07 MQTT signing key: %w", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("import BP07 MQTT signing key: %w", err)
	}
	if err := signingKey.Set("kid", "bp07-key"); err != nil {
		return nil, "", fmt.Errorf("set BP07 MQTT signing key ID: %w", err)
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set BP07 MQTT signing algorithm: %w", err)
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		return nil, "", fmt.Errorf("derive BP07 MQTT verification key: %w", err)
	}
	if err := publicKey.Set("kid", "bp07-key"); err != nil {
		return nil, "", fmt.Errorf("set BP07 MQTT verification key ID: %w", err)
	}
	if err := publicKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set BP07 MQTT verification algorithm: %w", err)
	}
	keySet := jwk.NewSet()
	if err := keySet.AddKey(publicKey); err != nil {
		return nil, "", fmt.Errorf("add BP07 MQTT verification key to JWKS: %w", err)
	}
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		return nil, "", fmt.Errorf("encode BP07 MQTT JWKS: %w", err)
	}
	token := jwt.New()
	// EMQX 5.8 compares configured claims as strings; a single audience remains
	// the same JWT audience when serialized in its RFC 7519 string form.
	token.Options().Enable(jwt.FlattenAudience)
	for name, value := range map[string]any{
		"iss": "https://issuer.example", "aud": []string{"narumq"},
		"sub": "bp07-client", "exp": time.Now().Add(2 * time.Hour),
	} {
		if err := token.Set(name, value); err != nil {
			return nil, "", fmt.Errorf("set BP07 MQTT JWT claim %q: %w", name, err)
		}
	}
	signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), signingKey))
	if err != nil {
		return nil, "", fmt.Errorf("sign BP07 MQTT access token: %w", err)
	}
	return jwksJSON, string(signedToken), nil
}
