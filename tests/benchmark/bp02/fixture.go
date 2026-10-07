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

type bp02Fixture struct {
	Directory                   string
	RunnerConfigDirectory       string
	NetworkOwnerConfigDirectory string
	EMQXConfigDirectory         string
	NodeConfigDirectories       []string
	CredentialsPath             string
	ManagementToken             string
}

type bp02ClusterConfig struct {
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
	BootstrapMembers     []bp02BootstrapMember `json:"bootstrapMembers"`
	SeedAddresses        []string              `json:"seedAddresses"`
	PeerTLS              bp02PeerTLSConfig     `json:"peerTLS"`
	Management           bp02ManagementConfig  `json:"management"`
	MQTT                 bp02MQTTConfig        `json:"mqtt"`
}

type bp02BootstrapMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type bp02PeerTLSConfig struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type bp02ManagementConfig struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
	AuditDirectory        string `json:"auditDirectory"`
}

type bp02MQTTConfig struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
}

type bp02SecurityConfig struct {
	JWT        bp02JWTConfig  `json:"jwt"`
	Identities []bp02Identity `json:"identities"`
	ACL        []bp02ACLRule  `json:"acl"`
}

type bp02JWTConfig struct {
	Issuer          string   `json:"issuer"`
	Audience        string   `json:"audience"`
	Algorithms      []string `json:"algorithms"`
	JWKSURL         string   `json:"jwksUrl"`
	CacheTTL        string   `json:"cacheTtl"`
	RefreshCooldown string   `json:"refreshCooldown"`
	ClockSkew       string   `json:"clockSkew"`
	HTTPTimeout     string   `json:"httpTimeout"`
}

type bp02Identity struct {
	Issuer           string   `json:"issuer"`
	Subject          string   `json:"subject"`
	TenantID         string   `json:"tenantId"`
	CredentialID     string   `json:"credentialId"`
	AllowedClientIDs []string `json:"allowedClientIds"`
	Roles            []string `json:"roles"`
}

type bp02ACLRule struct {
	TenantID    string `json:"tenantId"`
	Role        string `json:"role"`
	Effect      string `json:"effect"`
	Action      string `json:"action"`
	TopicFilter string `json:"topicFilter"`
}

type bp02TenantCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type bp02RunnerCredentials struct {
	Tenants         map[string]bp02TenantCredential `json:"tenants"`
	ManagementToken string                          `json:"managementToken"`
	CAFile          string                          `json:"caFile"`
}

type bp02CA struct {
	certificate *x509.Certificate
	privateKey  *ecdsa.PrivateKey
}

type bp02NodeCertificate struct {
	certificatePEM []byte
	privateKeyPEM  []byte
	fingerprint    string
}

func newBP02Fixture(profile Profile, runID string) (*bp02Fixture, error) {
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("validate BP02 fixture profile: %w", err)
	}
	if runID == "" {
		return nil, fmt.Errorf("BP02 fixture run ID is required")
	}
	directory, err := os.MkdirTemp("", "narumq-bp02-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary BP02 fixture directory: %w", err)
	}
	fixture := &bp02Fixture{
		Directory:                   directory,
		RunnerConfigDirectory:       filepath.Join(directory, "runner"),
		NetworkOwnerConfigDirectory: filepath.Join(directory, "network-owner"),
		EMQXConfigDirectory:         filepath.Join(directory, "emqx"),
		NodeConfigDirectories:       make([]string, bp02NodeCount),
	}
	cleanup := func(err error) (*bp02Fixture, error) {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return cleanup(fmt.Errorf("secure BP02 fixture directory: %w", err))
	}
	for _, path := range []string{fixture.RunnerConfigDirectory, fixture.NetworkOwnerConfigDirectory, fixture.EMQXConfigDirectory} {
		if err := os.Mkdir(path, 0o700); err != nil {
			return cleanup(fmt.Errorf("create BP02 fixture directory %q: %w", path, err))
		}
	}
	const emqxACL = "{allow, {username, \"bp02-a\"}, all, [\"bp02/a/#\", \"$share/bp02a/bp02/a/#\"]}.\n{allow, {username, \"bp02-b\"}, all, [\"bp02/b/#\"]}.\n{deny, all}.\n"
	if err := writeBP02FixtureFile(filepath.Join(fixture.EMQXConfigDirectory, "acl.conf"), []byte(emqxACL)); err != nil {
		return cleanup(fmt.Errorf("write BP02 EMQX ACL: %w", err))
	}

	adminBytes := make([]byte, 32)
	if _, err := rand.Read(adminBytes); err != nil {
		return cleanup(fmt.Errorf("generate BP02 management token: %w", err))
	}
	fixture.ManagementToken = hex.EncodeToString(adminBytes)
	ca, err := newBP02CA()
	if err != nil {
		return cleanup(err)
	}
	jwksJSON, accessToken, err := newBP02JWTFixture()
	if err != nil {
		return cleanup(err)
	}
	certificates := make([]bp02NodeCertificate, bp02NodeCount)
	members := make([]bp02BootstrapMember, bp02NodeCount)
	for index := range certificates {
		nodeID := index + 1
		dnsName := fmt.Sprintf("node%d.bp02.test", nodeID)
		certificates[index], err = newBP02NodeCertificate(ca, dnsName)
		if err != nil {
			return cleanup(err)
		}
		members[index] = bp02BootstrapMember{
			MemberID: uint64(nodeID), Address: dnsName + ":7000", DNSSAN: dnsName,
			CertificateSHA256: certificates[index].fingerprint,
		}
	}
	security := bp02SecurityConfig{
		JWT: bp02JWTConfig{
			Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
			JWKSURL: "http://127.0.0.1:18080/jwks.json", CacheTTL: "5m",
			RefreshCooldown: "30s", ClockSkew: "30s", HTTPTimeout: "5s",
		},
		Identities: []bp02Identity{
			{Issuer: "https://issuer.example", Subject: "bp02-a", TenantID: "bp02-a",
				CredentialID: "bp02-a-credential", AllowedClientIDs: bp02AllowedClientIDs(profile, "bp02-a"), Roles: []string{"client"}},
			{Issuer: "https://issuer.example", Subject: "bp02-b", TenantID: "bp02-b",
				CredentialID: "bp02-b-credential", AllowedClientIDs: bp02AllowedClientIDs(profile, "bp02-b"), Roles: []string{"client"}},
		},
		ACL: []bp02ACLRule{
			{TenantID: "bp02-a", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "bp02/a/#"},
			{TenantID: "bp02-a", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "bp02/a/#"},
			{TenantID: "bp02-b", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "bp02/b/#"},
			{TenantID: "bp02-b", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "bp02/b/#"},
		},
	}
	securityJSON, err := json.Marshal(security)
	if err != nil {
		return cleanup(fmt.Errorf("encode BP02 MQTT security config: %w", err))
	}
	if err := writeBP02FixtureFile(filepath.Join(fixture.RunnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP02 runner JWKS: %w", err))
	}
	if err := writeBP02FixtureFile(filepath.Join(fixture.NetworkOwnerConfigDirectory, "jwks.json"), jwksJSON); err != nil {
		return cleanup(fmt.Errorf("write BP02 network owner JWKS: %w", err))
	}

	caFile := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certificate.Raw})
	serverPEM, serverKeyPEM, err := newBP02EMQXServerCertificate(ca)
	if err != nil {
		return cleanup(err)
	}
	for name, contents := range map[string][]byte{"ca.pem": caFile, "server.pem": serverPEM, "server-key.pem": serverKeyPEM} {
		if err := writeBP02FixtureFile(filepath.Join(fixture.EMQXConfigDirectory, name), contents); err != nil {
			return cleanup(err)
		}
	}
	if err := writeBP02FixtureFile(filepath.Join(fixture.RunnerConfigDirectory, "ca.pem"), caFile); err != nil {
		return cleanup(err)
	}
	for index, certificate := range certificates {
		nodeNumber := index + 1
		dnsName := fmt.Sprintf("node%d.bp02.test", nodeNumber)
		configDir := filepath.Join(directory, fmt.Sprintf("node-%d", nodeNumber))
		if err := os.Mkdir(configDir, 0o700); err != nil {
			return cleanup(fmt.Errorf("create BP02 node %d config directory: %w", nodeNumber, err))
		}
		fixture.NodeConfigDirectories[index] = configDir
		for name, contents := range map[string][]byte{
			"ca.pem": caFile, "node.pem": certificate.certificatePEM,
			"node-key.pem": certificate.privateKeyPEM, "jwks.json": jwksJSON,
			"admin.token": []byte(fixture.ManagementToken + "\n"), "mqtt-security.json": securityJSON,
		} {
			if err := writeBP02FixtureFile(filepath.Join(configDir, name), contents); err != nil {
				return cleanup(fmt.Errorf("write BP02 node %d fixture %s: %w", nodeNumber, name, err))
			}
		}
		config := bp02ClusterConfig{
			Mode: "cluster", ClusterID: bp02ClusterID, NodeID: uint64(nodeNumber),
			Directory: "/var/lib/narumq/node", ListenAddress: "0.0.0.0:7000",
			AdvertisedAddress: dnsName + ":7000", LocalDNSName: dnsName,
			ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
			Role: "bootstrap", BootstrapMembers: members, SeedAddresses: []string{},
			PeerTLS:    bp02PeerTLSConfig{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"},
			Management: bp02ManagementConfig{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token", AuditDirectory: "/var/lib/narumq/audit"},
			MQTT:       bp02MQTTConfig{TCPListenAddress: "0.0.0.0:1883", TLSListenAddress: "0.0.0.0:8883", SecurityConfigFile: "/config/mqtt-security.json"},
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return cleanup(fmt.Errorf("encode BP02 node %d cluster config: %w", nodeNumber, err))
		}
		if err := writeBP02FixtureFile(filepath.Join(configDir, "node.json"), configJSON); err != nil {
			return cleanup(fmt.Errorf("write BP02 node %d cluster config: %w", nodeNumber, err))
		}
	}

	fixture.CredentialsPath = filepath.Join(fixture.RunnerConfigDirectory, "credentials.json")
	credentialsJSON, err := json.Marshal(bp02RunnerCredentials{
		Tenants: map[string]bp02TenantCredential{
			"bp02-a": {Username: "bp02-a", Password: accessToken["bp02-a"]},
			"bp02-b": {Username: "bp02-b", Password: accessToken["bp02-b"]},
		},
		ManagementToken: fixture.ManagementToken, CAFile: "/runner-config/ca.pem",
	})
	if err != nil {
		return cleanup(fmt.Errorf("encode BP02 runner credentials: %w", err))
	}
	if err := writeBP02FixtureFile(fixture.CredentialsPath, credentialsJSON); err != nil {
		return cleanup(fmt.Errorf("write BP02 runner credentials: %w", err))
	}
	return fixture, nil
}

func (fixture *bp02Fixture) Close() error {
	if fixture == nil || fixture.Directory == "" {
		return nil
	}
	directory := fixture.Directory
	fixture.Directory = ""
	return os.RemoveAll(directory)
}

func writeBP02FixtureFile(path string, contents []byte) error {
	return os.WriteFile(path, contents, 0o600)
}

func newBP02CA() (bp02CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp02CA{}, fmt.Errorf("generate BP02 cluster CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp02CA{}, fmt.Errorf("generate BP02 cluster CA serial: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "NaruMQ BP02 CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return bp02CA{}, fmt.Errorf("create BP02 cluster CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp02CA{}, fmt.Errorf("parse BP02 cluster CA certificate: %w", err)
	}
	return bp02CA{certificate: certificate, privateKey: key}, nil
}

func newBP02NodeCertificate(ca bp02CA, dnsName string) (bp02NodeCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return bp02NodeCertificate{}, fmt.Errorf("generate BP02 node certificate key for %s: %w", dnsName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return bp02NodeCertificate{}, fmt.Errorf("generate BP02 node certificate serial for %s: %w", dnsName, err)
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
		return bp02NodeCertificate{}, fmt.Errorf("create BP02 node certificate for %s: %w", dnsName, err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return bp02NodeCertificate{}, fmt.Errorf("parse BP02 node certificate for %s: %w", dnsName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return bp02NodeCertificate{}, fmt.Errorf("marshal BP02 node key for %s: %w", dnsName, err)
	}
	fingerprint := sha256.Sum256(certificate.Raw)
	return bp02NodeCertificate{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		fingerprint:    hex.EncodeToString(fingerprint[:]),
	}, nil
}

func newBP02JWTFixture() ([]byte, map[string]string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("generate BP02 MQTT signing key: %w", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		return nil, nil, err
	}
	if err := signingKey.Set("kid", "bp02-key"); err != nil {
		return nil, nil, err
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		return nil, nil, err
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		return nil, nil, err
	}
	keySet := jwk.NewSet()
	if err := keySet.AddKey(publicKey); err != nil {
		return nil, nil, err
	}
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		return nil, nil, err
	}
	tokens := make(map[string]string, 2)
	for _, tenant := range []string{"bp02-a", "bp02-b"} {
		token := jwt.New()
		token.Options().Enable(jwt.FlattenAudience)
		for name, value := range map[string]any{
			"iss": "https://issuer.example", "aud": []string{"narumq"},
			"sub": tenant, "exp": time.Now().Add(2 * time.Hour),
		} {
			if err := token.Set(name, value); err != nil {
				return nil, nil, err
			}
		}
		signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), signingKey))
		if err != nil {
			return nil, nil, err
		}
		tokens[tenant] = string(signedToken)
	}
	return jwksJSON, tokens, nil
}

func newBP02EMQXServerCertificate(ca bp02CA) (certificatePEM, privateKeyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "BP02 EMQX"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(2 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"node1.bp02.test", "node2.bp02.test", "node3.bp02.test"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.privateKey)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}
