package main

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

func TestNewBP03FixtureWritesContractAndCleansUp(t *testing.T) {
	profile, _, _, err := LoadProfile("testdata/smoke.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := newBP03Fixture(profile, "bp03-test-run")
	if err != nil {
		t.Fatal(err)
	}
	directory := fixture.Directory
	t.Cleanup(func() {
		if fixture.Directory != "" {
			_ = fixture.Close()
		}
	})
	if !strings.HasPrefix(filepath.Base(directory), "narumq-bp03-") {
		t.Fatalf("fixture directory %q does not use BP03 temp prefix", directory)
	}
	if len(fixture.NodeConfigDirectories) != 3 {
		t.Fatalf("got %d node config directories, want 3", len(fixture.NodeConfigDirectories))
	}
	if fixture.CredentialsPath != filepath.Join(fixture.RunnerConfigDirectory, "credentials.json") {
		t.Fatalf("credentials path %q is outside runner fixture directory", fixture.CredentialsPath)
	}
	if fixture.NetworkOwnerConfigDirectory != filepath.Join(fixture.Directory, "network-owner") {
		t.Fatalf("network owner config path %q is outside dedicated fixture directory", fixture.NetworkOwnerConfigDirectory)
	}
	publicFiles, err := os.ReadDir(fixture.NetworkOwnerConfigDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(publicFiles) != 1 || publicFiles[0].Name() != "jwks.json" || publicFiles[0].IsDir() {
		t.Fatalf("network owner public config contents = %v, want exactly jwks.json", publicFiles)
	}
	publicJWKS, err := os.ReadFile(filepath.Join(fixture.NetworkOwnerConfigDirectory, "jwks.json"))
	if err != nil {
		t.Fatal(err)
	}
	publicOwnerKeys, err := jwk.Parse(publicJWKS)
	if err != nil || publicOwnerKeys.Len() != 1 {
		t.Fatalf("network owner public JWKS keys = %v, error = %v; want one valid public key", publicOwnerKeys, err)
	}

	wantFiles := []string{"admin.token", "ca.pem", "jwks.json", "mqtt-security.json", "node-key.pem", "node.json", "node.pem"}

	var wantClientIDs []string
	for publisherNode := 1; publisherNode <= bp03NodeCount; publisherNode++ {
		for subscriberNode := 1; subscriberNode <= bp03NodeCount; subscriberNode++ {
			if publisherNode == subscriberNode {
				continue
			}
			pair := fmt.Sprintf("%d-%d", publisherNode, subscriberNode)
			wantClientIDs = append(wantClientIDs, "bp03-probe-pub-"+pair, "bp03-probe-sub-"+pair)
		}
	}
	for qos := 1; qos <= 2; qos++ {
		for pair := 1; pair <= profile.PairsPerQoS; pair++ {
			stream := fmt.Sprintf("q%d-%02d", qos, pair)
			wantClientIDs = append(wantClientIDs, "bp03-pub-"+stream, "bp03-sub-"+stream)
		}
	}
	var expectedMembers []bp03BootstrapMember
	var publicKeys jwk.Set
	for index, configDir := range fixture.NodeConfigDirectories {
		nodeNumber := index + 1
		dnsName := fmt.Sprintf("node%d.bp03.test", nodeNumber)
		entries, err := os.ReadDir(configDir)
		if err != nil {
			t.Fatal(err)
		}
		var gotFiles []string
		for _, entry := range entries {
			gotFiles = append(gotFiles, entry.Name())
		}
		sort.Strings(gotFiles)
		if !reflect.DeepEqual(gotFiles, wantFiles) {
			t.Errorf("node %d files = %v, want %v", nodeNumber, gotFiles, wantFiles)
		}

		caPEM, err := os.ReadFile(filepath.Join(configDir, "ca.pem"))
		if err != nil {
			t.Fatal(err)
		}
		caBlock, _ := pem.Decode(caPEM)
		if caBlock == nil || caBlock.Type != "CERTIFICATE" {
			t.Fatalf("node %d CA file is not certificate PEM", nodeNumber)
		}
		ca, err := x509.ParseCertificate(caBlock.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		if !ca.IsCA || ca.PublicKeyAlgorithm != x509.ECDSA {
			t.Errorf("node %d CA is not an ECDSA CA", nodeNumber)
		}
		nodePEM, err := os.ReadFile(filepath.Join(configDir, "node.pem"))
		if err != nil {
			t.Fatal(err)
		}
		nodeBlock, _ := pem.Decode(nodePEM)
		if nodeBlock == nil || nodeBlock.Type != "CERTIFICATE" {
			t.Fatalf("node %d certificate file is not PEM", nodeNumber)
		}
		nodeCert, err := x509.ParseCertificate(nodeBlock.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		if nodeCert.PublicKeyAlgorithm != x509.ECDSA || !reflect.DeepEqual(nodeCert.DNSNames, []string{dnsName, "localhost"}) {
			t.Errorf("node %d cert SANs/key = %v/%v", nodeNumber, nodeCert.DNSNames, nodeCert.PublicKeyAlgorithm)
		}
		if err := nodeCert.CheckSignatureFrom(ca); err != nil {
			t.Errorf("node %d cert is not signed by fixture CA: %v", nodeNumber, err)
		}
		keyPEM, err := os.ReadFile(filepath.Join(configDir, "node-key.pem"))
		if err != nil {
			t.Fatal(err)
		}
		keyBlock, _ := pem.Decode(keyPEM)
		if keyBlock == nil || keyBlock.Type != "EC PRIVATE KEY" {
			t.Errorf("node %d private key is not EC PEM", nodeNumber)
		}
		fingerprint := sha256.Sum256(nodeCert.Raw)
		member := bp03BootstrapMember{
			MemberID: uint64(nodeNumber), Address: dnsName + ":7000", DNSSAN: dnsName,
			CertificateSHA256: hex.EncodeToString(fingerprint[:]),
		}
		expectedMembers = append(expectedMembers, member)

		var config map[string]json.RawMessage
		readJSONFile(t, filepath.Join(configDir, "node.json"), &config)
		assertJSONKeys(t, "node.json", config, []string{
			"mode", "clusterId", "nodeId", "directory", "listenAddress", "advertisedAddress", "localDnsName",
			"clusterProtocolMajor", "clusterProtocolMinor", "stateSchemaMajor", "stateSchemaMinor", "role",
			"bootstrapMembers", "seedAddresses", "peerTLS", "management", "mqtt",
		})
		var nested struct {
			BootstrapMembers []map[string]json.RawMessage `json:"bootstrapMembers"`
			PeerTLS          map[string]json.RawMessage   `json:"peerTLS"`
			Management       map[string]json.RawMessage   `json:"management"`
			MQTT             map[string]json.RawMessage   `json:"mqtt"`
		}
		if err := json.Unmarshal(mustMarshal(t, config), &nested); err != nil {
			t.Fatal(err)
		}
		for memberIndex, member := range nested.BootstrapMembers {
			assertJSONKeys(t, fmt.Sprintf("bootstrapMembers[%d]", memberIndex), member,
				[]string{"memberId", "address", "dnsSan", "certificateSha256"})
		}
		assertJSONKeys(t, "peerTLS", nested.PeerTLS, []string{"certFile", "keyFile", "caFile"})
		assertJSONKeys(t, "management", nested.Management, []string{"listenAddress", "clusterAdminTokenFile", "auditDirectory"})
		assertJSONKeys(t, "mqtt", nested.MQTT, []string{"tcpListenAddress", "tlsListenAddress", "securityConfigFile"})
		var cluster bp03ClusterConfig
		if err := json.Unmarshal(mustMarshal(t, config), &cluster); err != nil {
			t.Fatal(err)
		}
		if cluster.Mode != "cluster" || cluster.ClusterID != "narumq-bp03" || cluster.NodeID != uint64(nodeNumber) ||
			cluster.Directory != "/var/lib/narumq/node" || cluster.ListenAddress != "0.0.0.0:7000" ||
			cluster.AdvertisedAddress != dnsName+":7000" || cluster.LocalDNSName != dnsName ||
			cluster.ClusterProtocolMajor != 2 || cluster.ClusterProtocolMinor != 0 ||
			cluster.StateSchemaMajor != 1 || cluster.StateSchemaMinor != 0 || cluster.Role != "bootstrap" {
			t.Errorf("node %d cluster config has unexpected fields: %+v", nodeNumber, cluster)
		}
		if !reflect.DeepEqual(cluster.BootstrapMembers, expectedMembers) && nodeNumber == 3 {
			t.Errorf("bootstrap members = %+v, want %v", cluster.BootstrapMembers, expectedMembers)
		}
		if len(cluster.BootstrapMembers) != 3 || len(cluster.SeedAddresses) != 0 {
			t.Errorf("node %d members/seeds = %d/%v", nodeNumber, len(cluster.BootstrapMembers), cluster.SeedAddresses)
		}
		if cluster.PeerTLS != (bp03PeerTLSConfig{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"}) ||
			cluster.Management != (bp03ManagementConfig{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token", AuditDirectory: "/var/lib/narumq/audit"}) ||
			cluster.MQTT != (bp03MQTTConfig{TCPListenAddress: "0.0.0.0:1883", SecurityConfigFile: "/config/mqtt-security.json"}) {
			t.Errorf("node %d TLS/management/MQTT config mismatch", nodeNumber)
		}
		adminToken, err := os.ReadFile(filepath.Join(configDir, "admin.token"))
		if err != nil {
			t.Fatal(err)
		}
		if string(adminToken) != fixture.ManagementToken+"\n" {
			t.Errorf("node %d admin token differs from fixture token", nodeNumber)
		}

		var security bp03SecurityConfig
		readJSONFile(t, filepath.Join(configDir, "mqtt-security.json"), &security)
		if !reflect.DeepEqual(security.JWT, bp03JWTConfig{
			Issuer: "https://issuer.example", Audience: "narumq", Algorithms: []string{"RS256"},
			JWKSURL: "http://127.0.0.1:18080/jwks.json", CacheTTL: "5m", RefreshCooldown: "30s", ClockSkew: "30s", HTTPTimeout: "5s",
		}) {
			t.Errorf("node %d JWT config = %+v", nodeNumber, security.JWT)
		}
		if len(security.Identities) != 1 {
			t.Fatalf("node %d identity count %d, want 1", nodeNumber, len(security.Identities))
		}
		identity := security.Identities[0]
		if identity.Issuer != "https://issuer.example" || identity.Subject != "bp03-client" || identity.TenantID != "bp03" ||
			identity.CredentialID != "bp03-client-credential" || !reflect.DeepEqual(identity.Roles, []string{"client"}) ||
			!reflect.DeepEqual(identity.AllowedClientIDs, wantClientIDs) {
			t.Errorf("node %d identity = %+v", nodeNumber, identity)
		}
		wantACL := []bp03ACLRule{
			{TenantID: "bp03", Role: "client", Effect: "allow", Action: "subscribe", TopicFilter: "bp03/#"},
			{TenantID: "bp03", Role: "client", Effect: "allow", Action: "publish", TopicFilter: "bp03/#"},
		}
		if !reflect.DeepEqual(security.ACL, wantACL) {
			t.Errorf("node %d ACL = %+v, want %+v", nodeNumber, security.ACL, wantACL)
		}
		jwksJSON, err := os.ReadFile(filepath.Join(configDir, "jwks.json"))
		if err != nil {
			t.Fatal(err)
		}
		keySet, err := jwk.Parse(jwksJSON)
		if err != nil {
			t.Fatalf("parse node %d JWKS: %v", nodeNumber, err)
		}
		if keySet.Len() != 1 {
			t.Errorf("node %d JWKS key count = %d, want 1", nodeNumber, keySet.Len())
		}
		publicKeys = keySet
	}

	runnerJWKSJSON, err := os.ReadFile(filepath.Join(fixture.RunnerConfigDirectory, "jwks.json"))
	if err != nil {
		t.Fatal(err)
	}
	runnerJWKS, err := jwk.Parse(runnerJWKSJSON)
	if err != nil {
		t.Fatalf("parse runner JWKS: %v", err)
	}
	if runnerJWKS.Len() != 1 {
		t.Errorf("runner JWKS key count = %d, want 1", runnerJWKS.Len())
	}

	var credentials bp03RunnerCredentials
	readJSONFile(t, fixture.CredentialsPath, &credentials)
	if credentials.Username != "jwt" || credentials.Password == "" || credentials.ManagementToken != fixture.ManagementToken {
		t.Fatal("runner credentials do not match fixture contract")
	}
	if len(fixture.ManagementToken) != 64 {
		t.Fatalf("management token length %d, want 64 hex characters", len(fixture.ManagementToken))
	}
	if decoded, err := hex.DecodeString(fixture.ManagementToken); err != nil || len(decoded) != 32 {
		t.Fatalf("management token is not 32 random bytes in hex: %v", err)
	}
	parsedToken, err := jwt.Parse([]byte(credentials.Password), jwt.WithKeySet(publicKeys), jwt.WithValidate(true))
	if err != nil {
		t.Fatalf("parse and verify fixture JWT: %v", err)
	}
	if subject, _ := parsedToken.Subject(); subject != "bp03-client" {
		t.Errorf("JWT subject = %q, want bp03-client", subject)
	}

	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Errorf("fixture directory still exists after Close: stat error %v", err)
	}
}

func readJSONFile(t *testing.T, path string, destination any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, destination); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func assertJSONKeys(t *testing.T, label string, fields map[string]json.RawMessage, want []string) {
	t.Helper()
	got := make([]string, 0, len(fields))
	for key := range fields {
		got = append(got, key)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s keys = %v, want %v", label, got, want)
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
