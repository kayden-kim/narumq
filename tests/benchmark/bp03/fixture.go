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

const bp03NodeCount = 3

type bp03Fixture struct {
	Directory                   string
	RunnerConfigDirectory       string
	NetworkOwnerConfigDirectory string
	EMQXConfigDirectory         string
	NodeConfigDirectories       []string
	CredentialsPath             string
	ManagementToken             string
}

type bp03ClusterConfig struct {
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
	BootstrapMembers     []bp03BootstrapMember `json:"bootstrapMembers"`
	SeedAddresses        []string              `json:"seedAddresses"`
	PeerTLS              bp03PeerTLSConfig     `json:"peerTLS"`
	Management           bp03ManagementConfig  `json:"management"`
	MQTT                 bp03MQTTConfig        `json:"mqtt"`
}

type bp03BootstrapMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type bp03PeerTLSConfig struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type bp03ManagementConfig struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
	AuditDirectory        string `json:"auditDirectory"`
}

type bp03MQTTConfig struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
}

type bp03SecurityConfig struct {
	JWT        bp03JWTConfig  `json:"jwt"`
	Identities []bp03Identity `json:"identities"`
	ACL        []bp03ACLRule  `json:"acl"`
}

type bp03JWTConfig struct {
	Issuer                string   `json:"issuer"`
	Audience              string   `json:"audience"`
	Algorithms            []string `json:"algorithms"`
	JWKSURL               string   `json:"jwksUrl"`
	CacheTTLMillis        uint32   `json:"cacheTtlMillis"`
	RefreshCooldownMillis uint32   `json:"refreshCooldownMillis"`
	ClockSkewMillis       uint32   `json:"clockSkewMillis"`
	HTTPTimeoutMillis     uint32   `json:"httpTimeoutMillis"`
}

type bp03Identity struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type bp03ACLRule struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

type bp03RunnerCredentials struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	ManagementToken string `json:"managementToken"`
}

type bp03CA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
	pem         []byte
}

type bp03NodeCertificate struct {
	certificatePEM []byte
	privateKeyPEM  []byte
	fingerprint    string
}

func newBP03Fixture(profile Profile, runID string) (*bp03Fixture, error) {
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("validate BP03 fixture profile: %w", err)
	}
	if runID == "" {
		return nil, fmt.Errorf("BP03 fixture run ID is required")
	}
	directory, err := os.MkdirTemp("", "narumq-bp03-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary BP03 fixture directory: %w", err)
	}
	fixture := &bp03Fixture{
		Directory:                   directory,
		RunnerConfigDirectory:       filepath.Join(directory, "runner"),
		NetworkOwnerConfigDirectory: filepath.Join(directory, "network-owner"),
		EMQXConfigDirectory:         filepath.Join(directory, "emqx"),
		NodeConfigDirectories:       make([]string, bp03NodeCount),
	}
	cleanup := func(err error) (*bp03Fixture, error) {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return cleanup(fmt.Errorf("secure BP03 fixture directory: %w", err))
	}
	if err := os.Mkdir(fixture.RunnerConfigDirectory, 0o700); err != nil {
		return cleanup(fmt.Errorf("create BP03 runner config directory: %w", err))
	}
	if err := os.Mkdir(fixture.NetworkOwnerConfigDirectory, 0o700); err != nil {
		return cleanup(fmt.Errorf("create BP03 network owner public config directory: %w", err))
	}
	if err := os.Mkdir(fixture.EMQXConfigDirectory, 0o700); err != nil {
		return cleanup(fmt.Errorf("create BP03 EMQX config directory: %w", err))
	}
	const emqxACL = "{allow, {username, \"jwt\"}, all, [\"bp03/#\"]}.\n{deny, all}.\n"
	if err := writeBP03FixtureFile(filepath.Join(fixture.EMQXConfigDirectory, "acl.conf"), []byte(emqxACL)); err != nil {
		return cleanup(fmt.Errorf("write BP03 EMQX ACL: %w", err))
	}

	adminBytes := make([]byte, 32)
	if _, err := rand.Read(adminBytes); err != nil {
		return cleanup(fmt.Errorf("generate BP03 management token: %w", err))
	}
	fixture.ManagementToken = hex.EncodeToString(adminBytes)

	ca, err := newBP03CA()
	if err != nil {
		return cleanup(err)
	}
	jwksJSON, accessToken, err := newBP03JWTFixture()
	if err != nil {
		return cleanup(err)
	}
	certificates := make([]bp03NodeCertificate, bp03NodeCount)
	members := make([]bp03BootstrapMember, bp03NodeCount)
	for index := range certificates {
		nodeID := index + 1
		dnsName := fmt.Sprintf("node%d.bp03.test", nodeID)
		certificates[index], err = newBP03NodeCertificate(ca, dnsName)
		if err != nil {
			return cleanup(err)
		}
		members[index] = bp03BootstrapMember{
			MemberID: uint64(nodeID), Address: dnsName + ":7000", DNSSAN: dnsName,
			CertificateSHA256: certificates[index].fingerprint,
		}
	}

	var allowedClientIDs []string
	for publisherNode := 1; publisherNode <= bp03NodeCount; publisherNode++ {
		for subscriberNode := 1; subscriberNode <= bp03NodeCount; subscriberNode++ {
			if publisherNode == subscriberNode {
				continue
			}
			pair := fmt.Sprintf("%d-%d", publisherNode, subscriberNode)
			allowedClientIDs = append(allowedClientIDs, "bp03-probe-pub-"+pair, "bp03-probe-sub-"+pair)
		}
	}
	for qos := 1; qos <= 2; qos++ {
		for pair := 1; pair <= profile.PairsPerQoS; pair++ {
			stream := fmt.Sprintf("q%d-%02d", qos, pair)
			allowedClientIDs = append(allowedClientIDs,
				"bp03-pub-"+stream, "bp03-sub-"+stream)
		}
	}
	security := bp03SecurityConfig{
		JWT: bp03JWTConfig{
			Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
			JWKSURL: "http://127.0.0.1:18080/jwks.json", CacheTTLMillis: 300000,
			RefreshCooldownMillis: 30000, ClockSkewMillis: 30000, HTTPTimeoutMillis: 5000,
		},
		Identities: []bp03Identity{{
			Issuer: "https://issuer.example", Subject: "bp03-client", TenantID: "bp03",
			CredentialID: "bp03-client-credential", AllowedClientIDs: allowedClientIDs, Roles: []string{"client"},
		}},
		ACL: []bp03ACLRule{
			{TenantID: "bp03", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "bp03/#"},
			{TenantID: "bp03", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "bp03/#"},
		},
	}
	securityJSON, err := json.Marshal(security)
	if err != nil {
		return cleanup(fmt.Errorf("encode BP03 MQTT security config: %w", err))
	}
	if err := writeBP03FixtureFile(filepath.Join(fixture.RunnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP03 runner JWKS: %w", err))
	}
	if err := writeBP03FixtureFile(filepath.Join(fixture.NetworkOwnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP03 network owner JWKS: %w", err))
	}

	caFile := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certificate.Raw})
	for index, certificate := range certificates {
		nodeNumber := index + 1
		dnsName := fmt.Sprintf("node%d.bp03.test", nodeNumber)
		configDir := filepath.Join(directory, fmt.Sprintf("node-%d", nodeNumber))
		if err := os.Mkdir(configDir, 0o700); err != nil {
			return cleanup(fmt.Errorf("create BP03 node %d config directory: %w", nodeNumber, err))
		}
		fixture.NodeConfigDirectories[index] = configDir
		for name, contents := range map[string][]byte{
			"ca.pem": caFile, "node.pem": certificate.certificatePEM,
			"node-key.pem": certificate.privateKeyPEM, "jwks.json": jwksJSON,
			"admin.token": []byte(fixture.ManagementToken + "\n"), "mqtt-security.json": securityJSON,
		} {
			if err := writeBP03FixtureFile(filepath.Join(configDir, name), contents); err != nil {
				return cleanup(fmt.Errorf("write BP03 node %d fixture %s: %w", nodeNumber, name, err))
			}
		}
		config := bp03ClusterConfig{
			Mode: "cluster", ClusterID: "narumq-bp03", NodeID: uint64(nodeNumber),
			Directory: "/var/lib/narumq/node", ListenAddress: "0.0.0.0:7000",
			AdvertisedAddress: dnsName + ":7000", LocalDNSName: dnsName,
			ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
			Role: "bootstrap", BootstrapMembers: members, SeedAddresses: []string{},
			PeerTLS:    bp03PeerTLSConfig{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"},
			Management: bp03ManagementConfig{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token", AuditDirectory: "/var/lib/narumq/audit"},
			MQTT:       bp03MQTTConfig{TCPListenAddress: "0.0.0.0:1883", TLSListenAddress: "", SecurityConfigFile: "/config/mqtt-security.json"},
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return cleanup(fmt.Errorf("encode BP03 node %d cluster config: %w", nodeNumber, err))
		}
		if err := writeBP03FixtureFile(filepath.Join(configDir, "node.json"), configJSON); err != nil {
			return cleanup(fmt.Errorf("write BP03 node %d cluster config: %w", nodeNumber, err))
		}
	}

	fixture.CredentialsPath = filepath.Join(fixture.RunnerConfigDirectory, "credentials.json")
	credentialsJSON, err := json.Marshal(bp03RunnerCredentials{Username: "jwt", Password: accessToken, ManagementToken: fixture.ManagementToken})
	if err != nil {
		return cleanup(fmt.Errorf("encode BP03 runner credentials: %w", err))
	}
	if err := writeBP03FixtureFile(fixture.CredentialsPath, credentialsJSON); err != nil {
		return cleanup(fmt.Errorf("write BP03 runner credentials: %w", err))
	}
	return fixture, nil
}

func (fixture *bp03Fixture) Close() error {
	if fixture == nil || fixture.Directory == "" {
		return nil
	}
	directory := fixture.Directory
	fixture.Directory = ""
	return os.RemoveAll(directory)
}

func writeBP03FixtureFile(path string, contents []byte) error {
	return os.WriteFile(path, contents, 0o600)
}

func newBP03CA() (bp03CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp03CA{}, fmt.Errorf("generate BP03 cluster CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp03CA{}, fmt.Errorf("generate BP03 cluster CA serial: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "NaruMQ BP03 CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return bp03CA{}, fmt.Errorf("create BP03 cluster CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp03CA{}, fmt.Errorf("parse BP03 cluster CA certificate: %w", err)
	}
	return bp03CA{certificate: certificate, privateKey: key}, nil
}

func newBP03NodeCertificate(ca bp03CA, dnsName string) (bp03NodeCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp03NodeCertificate{}, fmt.Errorf("generate BP03 node certificate key for %s: %w", dnsName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp03NodeCertificate{}, fmt.Errorf("generate BP03 node certificate serial for %s: %w", dnsName, err)
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
		return bp03NodeCertificate{}, fmt.Errorf("create BP03 node certificate for %s: %w", dnsName, err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp03NodeCertificate{}, fmt.Errorf("parse BP03 node certificate for %s: %w", dnsName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return bp03NodeCertificate{}, fmt.Errorf("marshal BP03 node key for %s: %w", dnsName, err)
	}
	fingerprint := sha256.Sum256(certificate.Raw)
	return bp03NodeCertificate{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		fingerprint:    hex.EncodeToString(fingerprint[:]),
	}, nil
}

func newBP03JWTFixture() ([]byte, string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, "", fmt.Errorf("generate BP03 MQTT signing key: %w", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		return nil, "", fmt.Errorf("import BP03 MQTT signing key: %w", err)
	}
	if err := signingKey.Set("kid", "bp03-key"); err != nil {
		return nil, "", fmt.Errorf("set BP03 MQTT signing key ID: %w", err)
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set BP03 MQTT signing algorithm: %w", err)
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		return nil, "", fmt.Errorf("derive BP03 MQTT verification key: %w", err)
	}
	if err := publicKey.Set("kid", "bp03-key"); err != nil {
		return nil, "", fmt.Errorf("set BP03 MQTT verification key ID: %w", err)
	}
	if err := publicKey.Set("alg", "RS256"); err != nil {
		return nil, "", fmt.Errorf("set BP03 MQTT verification algorithm: %w", err)
	}
	keySet := jwk.NewSet()
	if err := keySet.AddKey(publicKey); err != nil {
		return nil, "", fmt.Errorf("add BP03 MQTT verification key to JWKS: %w", err)
	}
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		return nil, "", fmt.Errorf("encode BP03 MQTT JWKS: %w", err)
	}
	token := jwt.New()
	// EMQX 5.8 compares configured claims as strings; a single audience remains
	// the same JWT audience when serialized in its RFC 7519 string form.
	token.Options().Enable(jwt.FlattenAudience)
	for name, value := range map[string]any{
		"iss": "https://issuer.example", "aud": []string{"narumq"},
		"sub": "bp03-client", "exp": time.Now().Add(2 * time.Hour),
	} {
		if err := token.Set(name, value); err != nil {
			return nil, "", fmt.Errorf("set BP03 MQTT JWT claim %q: %w", name, err)
		}
	}
	signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), signingKey))
	if err != nil {
		return nil, "", fmt.Errorf("sign BP03 MQTT access token: %w", err)
	}
	return jwksJSON, string(signedToken), nil
}
