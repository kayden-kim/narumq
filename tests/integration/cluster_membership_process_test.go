package integration

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	clusterMembershipProcessClusterID         = "process-acceptance-cluster"
	clusterMembershipProcessRelease           = "0.0.0"
	clusterMembershipProcessAdmin             = "process-acceptance-admin-token"
	clusterMembershipProcessReadOnlyAPIKey    = "process-acceptance-readonly-api-key"
	clusterMembershipProcessMaxManagementRate = 1_000_000
)

var clusterMembershipProcessAddressLeases = struct {
	sync.Mutex
	addresses map[string]struct{}
}{addresses: make(map[string]struct{})}

var clusterMembershipProcessBinaries = struct {
	sync.Mutex
	directory string
	paths     map[string]string
}{paths: make(map[string]string)}

var clusterLegacyConfigBinaries sync.Map

func TestMain(m *testing.M) {
	directory, err := os.MkdirTemp("", "narumq-integration-binaries-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create integration binary directory: %v\n", err)
		os.Exit(1)
	}
	clusterMembershipProcessBinaries.directory = directory
	code := m.Run()
	if err := removeIntegrationBinaryDirectory(directory); err != nil {
		fmt.Fprintf(os.Stderr, "remove integration binary directory: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

// removeIntegrationBinaryDirectory retries because Windows can keep an exited
// child's executable image locked briefly after Wait returns.
func removeIntegrationBinaryDirectory(directory string) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := os.RemoveAll(directory)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type clusterMembershipProcessFixture struct {
	tempDir                 string
	repoRoot                string
	binary                  string
	ctx                     context.Context
	harness                 *clusterMembershipProcessHarness
	caPath                  string
	certificates            []clusterMembershipProcessCertificate
	peerAddresses           []string
	managementAddresses     []string
	mqttAddresses           []string
	tokenPath               string
	readOnlyAPIKeyPath      string
	securityPath            string
	accessToken             string
	bootstrapMembers        []clusterMembershipProcessMember
	omitPublishBackpressure bool
	enableReadOnlyAPIKey    bool
	managementBindAll       bool
}

func newClusterMembershipProcessFixture(t *testing.T, nodeCount int, buildTags string) *clusterMembershipProcessFixture {
	t.Helper()
	repoRoot := clusterMembershipProcessRepoRoot(t)
	tempDir := t.TempDir()
	binaryPath := clusterMembershipProcessSharedBinary(t, repoRoot, buildTags)

	clusterCtx, clusterCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(clusterCancel)
	harness := newClusterMembershipProcessHarness(t, clusterCtx, repoRoot, binaryPath)

	ca := newClusterMembershipProcessCA(t)
	certificates := make([]clusterMembershipProcessCertificate, nodeCount)
	for index := range certificates {
		certificates[index] = newClusterMembershipProcessCertificate(t, ca, fmt.Sprintf("node%d.example.test", index+1))
	}
	caPath := filepath.Join(tempDir, "certificates", "ca.pem")
	if err := os.MkdirAll(filepath.Dir(caPath), 0o700); err != nil {
		t.Fatalf("create certificate directory: %v", err)
	}
	clusterMembershipProcessWriteFile(t, caPath, ca.pem, 0o600)

	addresses := clusterMembershipProcessAddresses(t, 3*nodeCount)
	t.Cleanup(harness.cleanup)

	peerAddresses := addresses[0:nodeCount]
	managementAddresses := addresses[nodeCount : 2*nodeCount]
	mqttAddresses := addresses[2*nodeCount : 3*nodeCount]

	tokenPath := filepath.Join(tempDir, "cluster-admin.token")
	clusterMembershipProcessWriteFile(t, tokenPath, []byte(clusterMembershipProcessAdmin+"\n"), 0o600)
	readOnlyAPIKeyPath := filepath.Join(tempDir, "cluster-readonly-api-key")
	clusterMembershipProcessWriteFile(t, readOnlyAPIKeyPath, []byte(clusterMembershipProcessReadOnlyAPIKey), 0o600)

	securityPath := filepath.Join(tempDir, "mqtt-security.json")
	jwksURL, accessToken := clusterMembershipProcessJWTFixture(t)
	securityConfig := []byte(fmt.Sprintf(`{
  "jwt": {
    "issuer": "https://issuer.example",
    "audience": "narumq",
    "algorithms": ["RS256"],
    "jwksUrl": %q,
    "cacheTtl": "5m",
    "refreshCooldown": "30s",
    "clockSkew": "30s",
    "httpTimeout": "5s"
  },
  "identities": [{
    "issuer": "https://issuer.example",
    "subject": "mqtt-process-client",
    "tenantId": "acceptance",
    "credentialId": "mqtt-process-client-credential",
    "allowedClientIds": [
      "tl-sub-1",
      "tl-sub-2",
      "tl-sub-3",
      "tl-conn-1",
      "tl-conn-2",
      "ready-connection-client",
      "bp-sub",
      "bp-held",
      "bp-rejected",
      "bp-closed",
      "bp-other",
      "bp-disk",
      "bp-live",
      "bp-sentinel",
      "handoff-client",
      "retained-publisher",
      "retained-subscriber-source",
      "retained-subscriber-restored",
      "retained-subscriber-restarted",
      "retained-subscriber-joiner",
      "offline-subscriber",
      "offline-publisher",
      "live-qos0-subscriber-2",
      "live-qos0-subscriber-3",
      "live-qos0-publisher-1",
      "live-qos0-publisher-2",
      "live-qos0-publisher-3",
      "durable-takeover-sub",
      "durable-offline-sub",
      "durable-takeover-pub",
      "durable-takeover-qos2-pub",
      "durable-takeover-sentinel",
      "durable-ack-sub",
      "durable-ack-pub",
      "durable-no-quorum-1",
      "durable-no-quorum-2",
      "durable-no-quorum-3",
      "durable-backup-sub",
      "durable-backup-pub",
      "durable-will-sub",
      "durable-will-observer",
      "durable-will-pub-1",
      "durable-will-pub-2",
      "durable-will-sentinel",
      "meta-observer",
      "meta-publisher",
      "meta-crash",
      "meta-resume",
      "meta-takeover",
      "meta-delayed",
      "detach-crash",
      "detach-crash-pub-live",
      "detach-crash-pub-outage",
      "detach-crash-pub-after",
      "detach-crash-pub-sentinel",
      "ownership-takeover-client",
      "ownership-publisher-1",
      "ownership-publisher-3",
      "durable-fault-sub",
      "durable-fault-pub-q1",
      "durable-fault-pub-q2",
      "durable-fault-sentinel",
      "durable-fault-isolated-q1",
      "durable-fault-isolated-q2",
      "replay-sub",
      "replay-pub-1",
      "replay-pub-2",
      "replay-pub-4",
      "drain-v5-a",
      "drain-open-1",
      "drain-open-2",
      "drain-open-3",
      "drain-detached-c",
      "drain-new-rejected",
      "drain-after-restart",
      "drain-publisher",
      "drain-cancel-check",
      "dashboard-sub-v5",
      "dashboard-pub-v311",
      "dashboard-drain-v5",
      "dashboard-probe-v5",
      "dashboard-probe-v311",
      "matrix-pub",
      "matrix-exact-s0",
      "matrix-exact-s1",
      "matrix-exact-s2",
      "matrix-plus-s0",
      "matrix-plus-s1",
      "matrix-plus-s2",
      "matrix-hash-s0",
      "matrix-hash-s1",
      "matrix-hash-s2",
      "matrix-shared-a",
      "matrix-shared-b",
      "retain-v311",
      "retain-rh0",
      "retain-rh1",
      "retain-rh2",
      "rap-1",
      "rap-0",
      "nolocal-v5",
      "concurrent-held",
      "concurrent-race-1",
      "concurrent-race-2",
      "concurrent-race-3",
      "concurrent-race-4",
      "concurrent-race-5",
      "concurrent-race-6",
      "concurrent-race-7",
      "concurrent-race-8",
      "concurrent-race-9",
      "concurrent-race-10",
      "stale-owner",
      "stale-inflight",
      "stale-pub",
      "compat-sub",
      "compat-load",
      "compat-pub",
      "compat-retained-check",
      "archive-retained-pub",
      "archive-offline-sub",
      "archive-offline-pub",
      "archive-upgrade-retained",
      "drain-will-observer-1",
      "drain-will-observer-2",
      "drain-will-observer-3",
      "drain-will-v311",
      "drain-will-zero",
      "drain-will-expire",
      "drain-will-safe",
      "drain-will-normal",
      "rb-v5-00", "rb-v5-01", "rb-v5-02", "rb-v5-03",
      "rb-v5-04", "rb-v5-05", "rb-v5-06", "rb-v5-07",
      "rb-v5-08", "rb-v5-09", "rb-v5-10", "rb-v5-11",
      "rb-v5-12", "rb-v5-13", "rb-v5-14", "rb-v5-15",
      "rb-v3-00", "rb-v3-01", "rb-pub", "rb-probe", "rb-rejected",
      "archive-downgrade-retained",
      "retained-qos12-live",
      "retained-qos12-pub",
      "retained-qos12-pub-v5",
      "retained-qos12-nq-1",
      "retained-qos12-nq-2",
      "retained-qos12-replay-1",
      "retained-qos12-replay-2",
      "retained-qos12-replay-3",
      "retained-qos12-replay-4",
      "retained-qos12-replay-5",
      "retained-qos12-replay-6",
      "retained-qos12-replay-7",
      "retained-qos12-replay-8",
      "metrics-gauge-subscriber"
    ],
    "roles": ["client"]
  }],
  "acl": [{
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "handoff/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "retained/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "retained/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "offline/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "offline/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "archive/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "archive/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "live/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "live/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "durable-takeover/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "durable-takeover/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "durable-ack/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "durable-ack/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "durable-untargeted/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "durable-backup/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "durable-backup/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "durable-will/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "durable-will/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "will",
    "topicFilter": "durable-will/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "meta/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "meta/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "will",
    "topicFilter": "meta/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "drain-will/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "drain-will/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "will",
    "topicFilter": "drain-will/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "detach/crash/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "detach/crash/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "ownership/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "ownership/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "durable-fault/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "durable-fault/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "replay/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "replay/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "unsubscribe",
    "topicFilter": "replay/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "drain/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "drain/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "rebalance/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "rebalance/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "matrix/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "matrix/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "retain/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "retain/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "rap/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "rap/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "nolocal/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "nolocal/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "concurrent/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "concurrent/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "stale/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "stale/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "compat/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "compat/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "publish",
    "topicFilter": "bp/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "bp/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "subscribe",
    "topicFilter": "metrics/#"
  }, {
    "tenantId": "acceptance",
    "role": "client",
    "effect": "allow",
    "action": "unsubscribe",
    "topicFilter": "metrics/#"
  }]
}`, jwksURL))
	clusterMembershipProcessWriteFile(t, securityPath, securityConfig, 0o600)

	bootstrapMembers := make([]clusterMembershipProcessMember, 3)
	for index := range bootstrapMembers {
		bootstrapMembers[index] = clusterMembershipProcessMember{
			MemberID: uint64(index + 1), Address: peerAddresses[index],
			DNSSAN:            certificates[index].dnsName,
			CertificateSHA256: hex.EncodeToString(certificates[index].fingerprint[:]),
		}
	}

	return &clusterMembershipProcessFixture{
		tempDir:             tempDir,
		repoRoot:            repoRoot,
		binary:              binaryPath,
		ctx:                 clusterCtx,
		harness:             harness,
		caPath:              caPath,
		certificates:        certificates,
		peerAddresses:       peerAddresses,
		managementAddresses: managementAddresses,
		mqttAddresses:       mqttAddresses,
		tokenPath:           tokenPath,
		readOnlyAPIKeyPath:  readOnlyAPIKeyPath,
		securityPath:        securityPath,
		accessToken:         accessToken,
		bootstrapMembers:    bootstrapMembers,
	}
}

func clusterMembershipProcessSharedBinary(t *testing.T, repoRoot, buildTags string) string {
	t.Helper()
	clusterMembershipProcessBinaries.Lock()
	defer clusterMembershipProcessBinaries.Unlock()
	if path, found := clusterMembershipProcessBinaries.paths[buildTags]; found {
		return path
	}
	path := filepath.Join(clusterMembershipProcessBinaries.directory,
		fmt.Sprintf("narumq-%d", len(clusterMembershipProcessBinaries.paths)))
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	clusterMembershipProcessBuild(t, repoRoot, path, buildTags, "")
	clusterMembershipProcessBinaries.paths[buildTags] = path
	t.Logf("built shared broker binary: tags=%q path=%s", buildTags, path)
	return path
}

func clusterMembershipProcessBuild(t *testing.T, repoRoot, binaryPath, buildTags, ldflags string) {
	t.Helper()
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer buildCancel()
	args := []string{"build", "-o", binaryPath}
	if buildTags != "" {
		args = append(args, "-tags", buildTags)
	}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, "./cmd/narumq")
	build := exec.CommandContext(buildCtx, "go", args...)
	build.Dir = repoRoot
	buildOutput, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("build cmd/narumq executable: %v\n%s", err, buildOutput)
	}
}

func (f *clusterMembershipProcessFixture) buildBinary(t *testing.T, name, ldflags string) string {
	t.Helper()
	return f.buildBinaryFrom(t, f.repoRoot, name, ldflags)
}

func (f *clusterMembershipProcessFixture) buildBinaryFrom(t *testing.T, sourceDir, name, ldflags string) string {
	t.Helper()
	binaryPath := filepath.Join(f.tempDir, name)
	if runtime.GOOS == "windows" {
		binaryPath += ".exe"
	}
	clusterMembershipProcessBuild(t, sourceDir, binaryPath, "", ldflags)
	if filepath.Clean(sourceDir) != filepath.Clean(f.repoRoot) {
		clusterLegacyConfigBinaries.Store(binaryPath, struct{}{})
	}
	return binaryPath
}

func clusterProcessConfigForBinary(binary, configPath string) (string, error) {
	if _, legacy := clusterLegacyConfigBinaries.Load(binary); !legacy {
		return configPath, nil
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("read legacy cluster config: %w", err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("decode legacy cluster config: %w", err)
	}
	management, ok := config["management"].(map[string]any)
	if !ok {
		return "", errors.New("legacy cluster config management object is missing")
	}
	delete(management, "auditDirectory")
	data, err = json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("encode legacy cluster config: %w", err)
	}
	legacyPath := strings.TrimSuffix(configPath, filepath.Ext(configPath)) + "-legacy.json"
	if err := os.WriteFile(legacyPath, data, 0o600); err != nil {
		return "", fmt.Errorf("write legacy cluster config: %w", err)
	}
	return legacyPath, nil
}
func (f *clusterMembershipProcessFixture) bootstrapConfig(memberID uint64) string {
	index := int(memberID - 1)
	dataDir := filepath.Join(f.tempDir, fmt.Sprintf("node-%d-data", memberID))
	_ = os.MkdirAll(dataDir, 0o700)
	config := clusterMembershipProcessConfig{
		Mode: "cluster", ClusterID: clusterMembershipProcessClusterID,
		NodeID: memberID, Directory: dataDir,
		ListenAddress: f.peerAddresses[index], AdvertisedAddress: f.peerAddresses[index],
		LocalDNSName:         f.certificates[index].dnsName,
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
		Role: "bootstrap", BootstrapMembers: append([]clusterMembershipProcessMember(nil), f.bootstrapMembers...),
		SeedAddresses: []string{}, PeerTLS: clusterMembershipProcessTLS{
			CertFile: f.certificates[index].certPath(f.tempDir), KeyFile: f.certificates[index].keyPath(f.tempDir), CAFile: f.caPath,
		},
		Management: f.managementConfig(index, memberID),
		MQTT:       clusterMembershipProcessMQTT{TCPListenAddress: f.mqttAddresses[index], SecurityConfigFile: f.securityPath},
	}
	if !f.omitPublishBackpressure {
		config.PublishBackpressure = &clusterMembershipProcessPublishBackpressure{MinFreeBytes: 16 << 20, MaxCommitLagMillis: 1000}
	}
	clusterMembershipProcessWriteFile(f.harness.t, config.PeerTLS.CertFile, f.certificates[index].certPEM, 0o600)
	clusterMembershipProcessWriteFile(f.harness.t, config.PeerTLS.KeyFile, f.certificates[index].keyPEM, 0o600)
	configPath := filepath.Join(f.tempDir, fmt.Sprintf("node-%d.json", memberID))
	clusterMembershipProcessWriteJSON(f.harness.t, configPath, config)
	return configPath
}

func (f *clusterMembershipProcessFixture) joinerConfig(memberID uint64, seedIDs []uint64) string {
	index := int(memberID - 1)
	dataDir := filepath.Join(f.tempDir, fmt.Sprintf("node-%d-data", memberID))
	_ = os.MkdirAll(dataDir, 0o700)
	seeds := make([]string, 0, len(seedIDs))
	for _, seedID := range seedIDs {
		seeds = append(seeds, "localhost:"+clusterMembershipProcessPort(f.peerAddresses[seedID-1]))
	}
	config := clusterMembershipProcessConfig{
		Mode: "cluster", ClusterID: clusterMembershipProcessClusterID, NodeID: memberID,
		Directory: dataDir, ListenAddress: f.peerAddresses[index], AdvertisedAddress: f.peerAddresses[index],
		LocalDNSName:         f.certificates[index].dnsName,
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
		Role: "joiner", BootstrapMembers: []clusterMembershipProcessMember{},
		SeedAddresses: seeds,
		PeerTLS: clusterMembershipProcessTLS{
			CertFile: f.certificates[index].certPath(f.tempDir), KeyFile: f.certificates[index].keyPath(f.tempDir), CAFile: f.caPath,
		},
		Management: f.managementConfig(index, memberID),
		MQTT:       clusterMembershipProcessMQTT{TCPListenAddress: f.mqttAddresses[index], SecurityConfigFile: f.securityPath},
	}
	if !f.omitPublishBackpressure {
		config.PublishBackpressure = &clusterMembershipProcessPublishBackpressure{MinFreeBytes: 16 << 20, MaxCommitLagMillis: 1000}
	}
	clusterMembershipProcessWriteFile(f.harness.t, config.PeerTLS.CertFile, f.certificates[index].certPEM, 0o600)
	clusterMembershipProcessWriteFile(f.harness.t, config.PeerTLS.KeyFile, f.certificates[index].keyPEM, 0o600)
	configPath := filepath.Join(f.tempDir, fmt.Sprintf("node-%d.json", memberID))
	clusterMembershipProcessWriteJSON(f.harness.t, configPath, config)
	return configPath
}

func (f *clusterMembershipProcessFixture) managementConfig(index int, memberID uint64) clusterMembershipProcessManagement {
	listenAddress := f.managementAddresses[index]
	if f.managementBindAll {
		_, port, err := net.SplitHostPort(listenAddress)
		if err != nil {
			f.harness.t.Fatalf("split management address %q: %v", listenAddress, err)
		}
		listenAddress = net.JoinHostPort("0.0.0.0", port)
	}
	config := clusterMembershipProcessManagement{
		ListenAddress:         listenAddress,
		ClusterAdminTokenFile: f.tokenPath,
		AuditDirectory:        f.auditDirectory(memberID),
		RequestRate:           clusterMembershipProcessMaxManagementRate,
		AuthFailureRate:       clusterMembershipProcessMaxManagementRate,
	}
	if f.enableReadOnlyAPIKey {
		config.ReadOnlyAPIKeyFile = f.readOnlyAPIKeyPath
	}
	return config
}

func (f *clusterMembershipProcessFixture) auditDirectory(memberID uint64) string {
	return filepath.Join(f.tempDir, fmt.Sprintf("node-%d-audit", memberID))
}

func (f *clusterMembershipProcessFixture) stageRequest(memberID uint64) clusterMembershipProcessStageRequest {
	index := int(memberID - 1)
	cert := f.certificates[index]
	return clusterMembershipProcessStageRequest{
		MemberID: memberID, Address: f.peerAddresses[index], DNSSAN: cert.dnsName,
		CertificateSHA256:    hex.EncodeToString(cert.fingerprint[:]),
		ReleaseVersion:       clusterMembershipProcessRelease,
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
	}
}

func (f *clusterMembershipProcessFixture) stageRequestBody(memberID uint64) []byte {
	request := f.stageRequest(memberID)
	requestBody, err := json.Marshal(request)
	if err != nil {
		f.harness.t.Fatalf("encode stage request for member %d: %v", memberID, err)
	}
	return requestBody
}

// Traceability: CLU-03; docs/specs/cluster-session-and-delivery.md §3.1, §11.10.
func TestDynamicClusterLiveQoS0CrossNodeForwardingProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		process, err := fixture.harness.start(
			fmt.Sprintf("live QoS 0 bootstrap node %d", index+1),
			fixture.bootstrapConfig(uint64(index+1)),
			fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)

	subscriber2 := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[1], fixture.accessToken, "live-qos0-subscriber-2", "live/#",
	)
	defer subscriber2.Close()
	subscriber3 := clusterMembershipProcessOpenSubscribedSession(
		t, fixture.mqttAddresses[2], fixture.accessToken, "live-qos0-subscriber-3", "live/#",
	)
	defer subscriber3.Close()
	clusterMembershipProcessWaitForRouteProjectionClients(
		t, fixture.ctx, processes, "live/#", "live-qos0-subscriber-2", "live-qos0-subscriber-3",
	)

	const topic = "live/event"
	for _, testCase := range []struct {
		nodeIndex int
		clientID  string
		payload   string
	}{
		{nodeIndex: 0, clientID: "live-qos0-publisher-1", payload: "from-node-1"},
		{nodeIndex: 1, clientID: "live-qos0-publisher-2", payload: "from-node-2"},
	} {
		clusterMembershipProcessPublish(
			t, fixture.mqttAddresses[testCase.nodeIndex], fixture.accessToken, testCase.clientID, topic, []byte(testCase.payload),
		)
		clusterMembershipProcessRequireLivePublish(t, subscriber2, topic, testCase.payload)
		clusterMembershipProcessRequireLivePublish(t, subscriber3, topic, testCase.payload)
	}

	if err := subscriber2.Close(); err != nil {
		t.Fatalf("close subscriber on node 2 before owner move: %v", err)
	}
	movedSubscriber, present := clusterMembershipProcessReconnectSession(
		t, fixture.mqttAddresses[0], fixture.accessToken, "live-qos0-subscriber-2",
	)
	defer movedSubscriber.Close()
	if !present {
		t.Fatal("subscriber reconnect on node 1 returned SessionPresent=false")
	}
	clusterMembershipProcessWaitForRouteProjectionClients(
		t, fixture.ctx, processes, "live/#", "live-qos0-subscriber-2", "live-qos0-subscriber-3",
	)

	const movedPayload = "from-node-3"
	clusterMembershipProcessPublish(
		t, fixture.mqttAddresses[2], fixture.accessToken, "live-qos0-publisher-3", topic, []byte(movedPayload),
	)
	clusterMembershipProcessRequireLivePublish(t, movedSubscriber, topic, movedPayload)
	clusterMembershipProcessRequireLivePublish(t, subscriber3, topic, movedPayload)
}

func TestDynamicClusterMembershipProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 4, "")
	harness := fixture.harness
	clusterCtx := fixture.ctx
	bootstrapConfigs := []string{
		fixture.bootstrapConfig(1),
		fixture.bootstrapConfig(2),
		fixture.bootstrapConfig(3),
	}
	managementAddresses := fixture.managementAddresses
	mqttAddresses := fixture.mqttAddresses
	accessToken := fixture.accessToken

	bootstrapProcesses := make([]*clusterMembershipProcess, 3)
	// An empty-directory bootstrap voter waits for both peers before opening
	// Raft, management or MQTT listeners.
	first, err := harness.start("bootstrap node 1", bootstrapConfigs[0], managementAddresses[0])
	if err != nil {
		t.Fatalf("start first bootstrap process: %v", err)
	}
	bootstrapProcesses[0] = first
	time.Sleep(2 * time.Second)
	if err := first.isAlive(); err != nil {
		t.Fatalf("isolated bootstrap voter exited: %v", err)
	}
	if status, _, err := clusterMembershipProcessRequest(clusterCtx, http.MethodGet, first.managementURL()+"/health/live", nil, "", ""); err == nil {
		t.Fatalf("isolated empty bootstrap voter served management status=%d before peers answered", status)
	}
	if conn, err := net.DialTimeout("tcp", mqttAddresses[0], time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("isolated empty bootstrap voter opened MQTT before peers answered")
	}

	for index := 1; index < len(bootstrapProcesses); index++ {
		process, startErr := harness.start(fmt.Sprintf("bootstrap node %d", index+1), bootstrapConfigs[index], managementAddresses[index])
		if startErr != nil {
			t.Fatalf("start bootstrap process %d: %v", index+1, startErr)
		}
		bootstrapProcesses[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, clusterCtx, bootstrapProcesses)
	leaderID, leaderURL := clusterMembershipProcessFindLeader(t, clusterCtx, bootstrapProcesses, 3)
	if leaderID < 1 || leaderID > 3 {
		t.Fatalf("bootstrap elected unexpected leader member ID %d", leaderID)
	}

	joinerConfigPath := fixture.joinerConfig(4, []uint64{1, 2, 3})
	request := fixture.stageRequest(4)
	requestBody := fixture.stageRequestBody(4)
	// Compatibility and identity rejection: verify that incompatible profiles and reserved IDs are rejected before staging.
	for tcIndex, tc := range []struct {
		name     string
		modifier func(*clusterMembershipProcessStageRequest)
		wantCode int
		wantErr  string
	}{
		{
			name: "unsupported release version",
			modifier: func(r *clusterMembershipProcessStageRequest) {
				r.ReleaseVersion = "99.0.0"
			},
			wantCode: http.StatusUnprocessableEntity,
			wantErr:  "compatibility_unsupported",
		},
		{
			name: "unsupported cluster protocol",
			modifier: func(r *clusterMembershipProcessStageRequest) {
				r.ClusterProtocolMajor = 99
			},
			wantCode: http.StatusUnprocessableEntity,
			wantErr:  "compatibility_unsupported",
		},
		{
			name: "unsupported state schema",
			modifier: func(r *clusterMembershipProcessStageRequest) {
				r.StateSchemaMajor = 99
			},
			wantCode: http.StatusUnprocessableEntity,
			wantErr:  "compatibility_unsupported",
		},
		{
			name: "reserved existing member ID",
			modifier: func(r *clusterMembershipProcessStageRequest) {
				r.MemberID = 1
			},
			wantCode: http.StatusConflict,
			wantErr:  "member_id_reserved",
		},
	} {
		incompatible := request
		tc.modifier(&incompatible)
		incompatibleBody, _ := json.Marshal(incompatible)
		compatKey := fmt.Sprintf("compat-key-%d", tcIndex)
		compatStatus, compatBody, compatErr := clusterMembershipProcessRequest(clusterCtx, http.MethodPost,
			leaderURL+"/api/v1/cluster/members", incompatibleBody, clusterMembershipProcessAdmin, compatKey)
		if compatErr != nil {
			t.Fatalf("compatibility check %s: %v", tc.name, compatErr)
		}
		var errEnvelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if compatStatus != tc.wantCode || json.Unmarshal(compatBody, &errEnvelope) != nil || errEnvelope.Error.Code != tc.wantErr {
			t.Fatalf("compatibility check %s status=%d body=%s, want %d %s", tc.name, compatStatus, compatBody, tc.wantCode, tc.wantErr)
		}
	}
	clusterMembershipProcessWaitForClusterSize(t, clusterCtx, bootstrapProcesses, 3)

	const idempotencyKey = "process-acceptance-join-node-4"
	status, body, err := clusterMembershipProcessRequest(clusterCtx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", requestBody, clusterMembershipProcessAdmin, idempotencyKey)
	if err != nil {
		t.Fatalf("stage joiner through management API: %v", err)
	}
	firstStage, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode initial stage response: %v (status %d, body %s)", err, status, body)
	}
	if status != http.StatusAccepted || firstStage.Action != "ADD" || firstStage.MemberID != 4 || firstStage.Phase != "STAGED" || firstStage.CommittedIndex == 0 || firstStage.OperationID == "" {
		t.Fatalf("initial stage response status=%d operation=%+v body=%s, want committed 202 ADD/STAGED", status, firstStage, body)
	}

	status, body, err = clusterMembershipProcessRequest(clusterCtx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", requestBody, clusterMembershipProcessAdmin, idempotencyKey)
	if err != nil {
		t.Fatalf("retry committed stage request: %v", err)
	}
	retryStage, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil {
		t.Fatalf("decode stage retry response: %v (status %d, body %s)", err, status, body)
	}
	if status != http.StatusOK || retryStage.Action != "ADD" || retryStage.MemberID != 4 ||
		retryStage.Phase != "STAGED" || retryStage.CommittedIndex != firstStage.CommittedIndex ||
		retryStage.OperationID != firstStage.OperationID {
		t.Fatalf("stage retry status=%d operation=%+v, want 200 and unchanged committed ADD operation %+v", status, retryStage, firstStage)
	}

	joiner, err := harness.start("joiner node 4", joinerConfigPath, managementAddresses[3])
	if err != nil {
		t.Fatalf("start joiner process: %v", err)
	}
	clusterMembershipProcessWaitForOperation(t, clusterCtx, bootstrapProcesses[0], firstStage.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForStatus(t, clusterCtx, joiner, "/health/ready", http.StatusOK)
	clusterMembershipProcessWaitForOperation(t, clusterCtx, joiner, firstStage.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForClusterSize(t, clusterCtx, bootstrapProcesses, 4)
	clusterMembershipProcessAssertConvergedSummary(t, clusterCtx, bootstrapProcesses, joiner, 4)

	// Idempotent retry of stage after promotion: re-requesting with the original idempotency key
	// returns 200 with the same operation ID and updated SUCCEEDED phase. No duplicate config change.
	status, body, err = clusterMembershipProcessRequest(clusterCtx, http.MethodPost,
		leaderURL+"/api/v1/cluster/members", requestBody, clusterMembershipProcessAdmin, idempotencyKey)
	if err != nil {
		t.Fatalf("retry stage after promotion: %v", err)
	}
	promotedStageRetry, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil || status != http.StatusOK || promotedStageRetry.OperationID != firstStage.OperationID ||
		promotedStageRetry.Phase != "SUCCEEDED" || promotedStageRetry.MemberID != 4 {
		t.Fatalf("stage retry after promotion status=%d operation=%+v body=%s err=%v, want 200 SUCCEEDED with same operation ID",
			status, promotedStageRetry, body, err)
	}
	clusterMembershipProcessWaitForClusterSize(t, clusterCtx, bootstrapProcesses, 4)

	// Kill and reopen node 4 without changing its config, identity, or persistent
	// directory. Its ready probe and committed operation view must converge again.
	if err := joiner.isAlive(); err != nil {
		t.Fatalf("joiner exited before same-volume crash: %v", err)
	}
	if err := joiner.crash(5 * time.Second); err != nil {
		t.Fatalf("crash joiner before same-volume restart: %v", err)
	}
	restartedJoiner, err := harness.start("joiner node 4 restarted on same volume", joinerConfigPath, managementAddresses[3])
	if err != nil {
		t.Fatalf("restart joiner process: %v", err)
	}
	clusterMembershipProcessWaitForStatus(t, clusterCtx, restartedJoiner, "/health/ready", http.StatusOK)
	clusterMembershipProcessWaitForOperation(t, clusterCtx, restartedJoiner, firstStage.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForOperation(t, clusterCtx, bootstrapProcesses[0], firstStage.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForClusterSize(t, clusterCtx, bootstrapProcesses, 4)
	clusterMembershipProcessAssertConvergedSummary(t, clusterCtx, bootstrapProcesses, restartedJoiner, 4)

	processesByID := map[uint64]*clusterMembershipProcess{
		1: bootstrapProcesses[0], 2: bootstrapProcesses[1], 3: bootstrapProcesses[2], 4: restartedJoiner,
	}
	summary, summaryStatus, summaryBody, err := clusterMembershipProcessGetSummary(
		clusterCtx, restartedJoiner.managementURL(), clusterMembershipProcessAdmin)
	if err != nil || summaryStatus != http.StatusOK || summary.LeaderNodeID == nil {
		t.Fatalf("read current leader before planned removal: status=%d summary=%+v body=%s err=%v",
			summaryStatus, summary, summaryBody, err)
	}
	removalTargetID := uint64(3)
	currentLeaderID, err := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
	if err != nil || currentLeaderID < 1 || currentLeaderID > 4 {
		t.Fatalf("current leader ID %q is invalid: %v", *summary.LeaderNodeID, err)
	}
	leaderProcess := processesByID[currentLeaderID]
	const removalIdempotencyKey = "process-acceptance-remove-node-3"
	status, body, err = clusterMembershipProcessRequest(clusterCtx, http.MethodDelete,
		leaderProcess.managementURL()+"/api/v1/cluster/members/3",
		nil, clusterMembershipProcessAdmin, removalIdempotencyKey)
	if err != nil {
		t.Fatalf("start planned removal of member 3: %v", err)
	}
	removalOperation, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil || status != http.StatusAccepted || removalOperation.Action != "REMOVE" ||
		removalOperation.MemberID != removalTargetID || removalOperation.Phase != "REMOVING" ||
		removalOperation.CommittedIndex == 0 {
		t.Fatalf("planned removal response status=%d operation=%+v body=%s err=%v, want committed REMOVING",
			status, removalOperation, body, err)
	}
	var operationReader *clusterMembershipProcess
	for memberID, process := range processesByID {
		if memberID != removalTargetID {
			operationReader = process
			break
		}
	}
	clusterMembershipProcessWaitForOperation(t, clusterCtx, operationReader, removalOperation.OperationID, "SUCCEEDED")

	survivors := []*clusterMembershipProcess{processesByID[1], processesByID[2], processesByID[4]}
	survivorIDs := []uint64{1, 2, 4}
	clusterMembershipProcessWaitForClusterSize(t, clusterCtx, survivors, 3)
	clusterMembershipProcessAssertVotersAndRetired(t, clusterCtx, survivors, []string{"1", "2", "4"}, "3")

	survivorSummary, survivorStatus, survivorBody, err := clusterMembershipProcessGetSummary(
		clusterCtx, survivors[0].managementURL(), clusterMembershipProcessAdmin)
	if err != nil || survivorStatus != http.StatusOK || survivorSummary.LeaderNodeID == nil {
		t.Fatalf("read leader after planned removal: status=%d summary=%+v body=%s err=%v",
			survivorStatus, survivorSummary, survivorBody, err)
	}
	retryLeaderID, err := strconv.ParseUint(*survivorSummary.LeaderNodeID, 10, 64)
	if err != nil || processesByID[retryLeaderID] == nil || retryLeaderID == removalTargetID {
		t.Fatalf("post-removal leader ID %q is invalid: %v", *survivorSummary.LeaderNodeID, err)
	}
	status, body, err = clusterMembershipProcessRequest(clusterCtx, http.MethodDelete,
		processesByID[retryLeaderID].managementURL()+"/api/v1/cluster/members/3",
		nil, clusterMembershipProcessAdmin, removalIdempotencyKey)
	if err != nil {
		t.Fatalf("retry planned removal with the same idempotency key: %v", err)
	}
	retryRemoval, err := clusterMembershipProcessDecodeOperation(body)
	if err != nil || status != http.StatusOK || retryRemoval.OperationID != removalOperation.OperationID ||
		retryRemoval.Phase != "SUCCEEDED" || retryRemoval.MemberID != removalTargetID {
		t.Fatalf("planned removal retry status=%d operation=%+v body=%s err=%v, want same SUCCEEDED operation",
			status, retryRemoval, body, err)
	}

	retiredBody := fixture.stageRequestBody(removalTargetID)
	status, body, err = clusterMembershipProcessRequest(clusterCtx, http.MethodPost,
		processesByID[retryLeaderID].managementURL()+"/api/v1/cluster/members",
		retiredBody, clusterMembershipProcessAdmin, "process-acceptance-reuse-retired-id")
	if err != nil {
		t.Fatalf("request retired member ID reuse: %v", err)
	}
	var errorEnvelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if status != http.StatusConflict || json.Unmarshal(body, &errorEnvelope) != nil ||
		errorEnvelope.Error.Code != "member_id_reserved" {
		t.Fatalf("retired member ID reuse status=%d body=%s, want 409 member_id_reserved", status, body)
	}
	mqttSessionOwnerID := retryLeaderID
	mqttSessionOwner := processesByID[mqttSessionOwnerID]
	clusterMembershipProcessWaitForStatus(t, clusterCtx, mqttSessionOwner, "/health/ready", http.StatusOK)
	clusterMembershipProcessAssertMQTTAccepted(t, mqttAddresses[mqttSessionOwnerID-1], accessToken)
	crashIndex := -1
	for index, memberID := range survivorIDs {
		if memberID != mqttSessionOwnerID {
			crashIndex = index
			break
		}
	}
	if crashIndex < 0 {
		t.Fatal("no remaining voter can be crashed without stopping the MQTT session owner")
	}
	if err := survivors[crashIndex].crash(5 * time.Second); err != nil {
		t.Fatalf("crash one of three remaining voters: %v", err)
	}
	remainingSurvivors := make([]*clusterMembershipProcess, 0, 2)
	for index, process := range survivors {
		if index != crashIndex {
			remainingSurvivors = append(remainingSurvivors, process)
		}
	}
	clusterMembershipProcessWaitForClusterSize(t, clusterCtx, remainingSurvivors, 3)
	clusterMembershipProcessWaitForStatus(t, clusterCtx, mqttSessionOwner, "/health/ready", http.StatusOK)
	clusterMembershipProcessAssertMQTTAccepted(t, mqttAddresses[mqttSessionOwnerID-1], accessToken)

	// Quorum loss: crash the second remaining voter so only 1 out of 3 voters survives.
	var secondCrashTarget, lastSurvivor *clusterMembershipProcess
	var lastSurvivorID uint64
	for _, process := range remainingSurvivors {
		if process == mqttSessionOwner {
			lastSurvivor = process
			lastSurvivorID = mqttSessionOwnerID
		} else {
			secondCrashTarget = process
		}
	}
	if secondCrashTarget == nil || lastSurvivor == nil {
		t.Fatal("could not identify survivor and crash target for quorum loss")
	}
	if err := secondCrashTarget.crash(5 * time.Second); err != nil {
		t.Fatalf("crash second voter to induce quorum loss: %v", err)
	}
	clusterMembershipProcessWaitForStatus(t, clusterCtx, lastSurvivor, "/health/ready", http.StatusServiceUnavailable)
	lossStatus, lossBody, lossErr := clusterMembershipProcessRequest(clusterCtx, http.MethodDelete,
		lastSurvivor.managementURL()+"/api/v1/cluster/members/1", nil, clusterMembershipProcessAdmin, "quorum-loss-remove")
	if lossStatus != http.StatusServiceUnavailable {
		t.Fatalf("removal during quorum loss status=%d body=%s err=%v, want 503", lossStatus, lossBody, lossErr)
	}
	clusterMembershipProcessAssertMQTTRejected(t, mqttAddresses[lastSurvivorID-1])
}

func clusterMembershipProcessAssertVotersAndRetired(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, wantVoters []string, wantRetired string) {
	t.Helper()
	for _, process := range processes {
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/api/v1/nodes", nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			t.Fatalf("%s list nodes request: %v", process.name, err)
		}
		if status != http.StatusOK {
			t.Fatalf("%s list nodes status=%d body=%s", process.name, status, body)
		}
		var envelope struct {
			Data []struct {
				NodeID string `json:"nodeId"`
				Role   string `json:"role"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatalf("%s decode list nodes: %v", process.name, err)
		}
		var voterIDs []string
		foundRetired := false
		for _, node := range envelope.Data {
			switch node.Role {
			case "LEADER", "FOLLOWER":
				voterIDs = append(voterIDs, node.NodeID)
			case "RETIRED":
				if node.NodeID == wantRetired {
					foundRetired = true
				}
			}
		}
		sort.Strings(voterIDs)
		expected := append([]string(nil), wantVoters...)
		sort.Strings(expected)
		if !slices.Equal(voterIDs, expected) {
			t.Fatalf("%s voter nodes = %v, want %v (raw = %s)", process.name, voterIDs, expected, body)
		}
		if wantRetired != "" && !foundRetired {
			t.Fatalf("%s did not find retired node %q in nodes list (raw = %s)", process.name, wantRetired, body)
		}
	}
}

type clusterMembershipProcessHarness struct {
	t         *testing.T
	ctx       context.Context
	root      string
	binary    string
	processes []*clusterMembershipProcess
}

func newClusterMembershipProcessHarness(t *testing.T, ctx context.Context, root, binary string) *clusterMembershipProcessHarness {
	return &clusterMembershipProcessHarness{t: t, ctx: ctx, root: root, binary: binary}
}

func (h *clusterMembershipProcessHarness) launch(name, configPath, managementAddress string, env []string) (*clusterMembershipProcess, error) {
	return h.launchBinary(h.binary, name, configPath, managementAddress, env)
}

func (h *clusterMembershipProcessHarness) launchBinary(binary, name, configPath, managementAddress string, env []string) (*clusterMembershipProcess, error) {
	configPath, err := clusterProcessConfigForBinary(binary, configPath)
	if err != nil {
		return nil, err
	}
	processCtx, cancel := context.WithCancel(h.ctx)
	command := exec.CommandContext(processCtx, binary, "-cluster-config", configPath)
	command.Dir = h.root
	if len(env) != 0 {
		command.Env = append(os.Environ(), env...)
	}
	logs := &clusterMembershipProcessLog{limit: 1 << 20}
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	process := &clusterMembershipProcess{
		name: name, command: command, cancel: cancel, logs: logs,
		done: make(chan struct{}), managementAddress: managementAddress,
	}
	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()
	h.processes = append(h.processes, process)
	return process, nil
}

func (h *clusterMembershipProcessHarness) start(name, configPath, managementAddress string) (*clusterMembershipProcess, error) {
	return h.launch(name, configPath, managementAddress, nil)
}

func (h *clusterMembershipProcessHarness) startBinary(binary, name, configPath, managementAddress string) (*clusterMembershipProcess, error) {
	return h.launchBinary(binary, name, configPath, managementAddress, nil)
}

func (h *clusterMembershipProcessHarness) startArmed(name, configPath, managementAddress, failpoint string) (*clusterMembershipProcess, error) {
	return h.launch(name, configPath, managementAddress, []string{"NARUMQ_FAILPOINT=" + failpoint})
}

func (h *clusterMembershipProcessHarness) cleanup() {
	for index := len(h.processes) - 1; index >= 0; index-- {
		if err := h.processes[index].stop(5 * time.Second); err != nil && h.t.Failed() {
			h.t.Logf("stop %s during cleanup: %v", h.processes[index].name, err)
		}
	}
	if h.t.Failed() {
		for _, process := range h.processes {
			h.t.Logf("%s output:\n%s", process.name, process.logs.String())
		}
	}
}

type clusterMembershipProcess struct {
	name              string
	command           *exec.Cmd
	cancel            context.CancelFunc
	logs              *clusterMembershipProcessLog
	done              chan struct{}
	waitErr           error
	managementAddress string
	stopOnce          sync.Once
	stopErr           error
}

func (process *clusterMembershipProcess) managementURL() string {
	return "http://" + process.managementAddress
}

func (process *clusterMembershipProcess) isAlive() error {
	select {
	case <-process.done:
		return fmt.Errorf("%s exited: %v\n%s", process.name, process.waitErr, process.logs.String())
	default:
		return nil
	}
}

func (process *clusterMembershipProcess) exitCode() (int, bool) {
	select {
	case <-process.done:
		if process.waitErr == nil {
			return 0, true
		}
		var exitErr *exec.ExitError
		if errors.As(process.waitErr, &exitErr) {
			return exitErr.ExitCode(), true
		}
		return -1, false
	default:
		return 0, false
	}
}

func (process *clusterMembershipProcess) stop(timeout time.Duration) error {
	process.stopOnce.Do(func() {
		process.cancel()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-process.done:
		case <-timer.C:
			if process.command.Process != nil {
				_ = process.command.Process.Kill()
			}
			select {
			case <-process.done:
			case <-time.After(timeout):
				process.stopErr = fmt.Errorf("%s did not exit after process kill", process.name)
			}
		}
	})
	return process.stopErr
}
func (process *clusterMembershipProcess) crash(timeout time.Duration) error {
	if process.command.Process == nil {
		return fmt.Errorf("%s has no process to crash", process.name)
	}
	if err := process.command.Process.Kill(); err != nil {
		select {
		case <-process.done:
			return nil
		default:
			return fmt.Errorf("kill %s: %w", process.name, err)
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.done:
		if process.waitErr == nil {
			return fmt.Errorf("%s exited cleanly after forced termination", process.name)
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("%s did not exit after forced termination", process.name)
	}
}

type clusterMembershipProcessLog struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (logs *clusterMembershipProcessLog) Write(data []byte) (int, error) {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	written := len(data)
	if len(data) >= logs.limit {
		logs.data = append(logs.data[:0], data[len(data)-logs.limit:]...)
		return written, nil
	}
	if overflow := len(logs.data) + len(data) - logs.limit; overflow > 0 {
		copy(logs.data, logs.data[overflow:])
		logs.data = logs.data[:len(logs.data)-overflow]
	}
	logs.data = append(logs.data, data...)
	return written, nil
}

func (logs *clusterMembershipProcessLog) String() string {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	return string(append([]byte(nil), logs.data...))
}

type clusterMembershipProcessConfig struct {
	Mode                 string                                       `json:"mode"`
	ClusterID            string                                       `json:"clusterId"`
	NodeID               uint64                                       `json:"nodeId"`
	Directory            string                                       `json:"directory"`
	ListenAddress        string                                       `json:"listenAddress"`
	AdvertisedAddress    string                                       `json:"advertisedAddress"`
	LocalDNSName         string                                       `json:"localDnsName"`
	ClusterProtocolMajor uint32                                       `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32                                       `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32                                       `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32                                       `json:"stateSchemaMinor"`
	Role                 string                                       `json:"role"`
	BootstrapMembers     []clusterMembershipProcessMember             `json:"bootstrapMembers"`
	SeedAddresses        []string                                     `json:"seedAddresses"`
	PeerTLS              clusterMembershipProcessTLS                  `json:"peerTLS"`
	Management           clusterMembershipProcessManagement           `json:"management"`
	MQTT                 clusterMembershipProcessMQTT                 `json:"mqtt"`
	TenantQuotas         *clusterMembershipProcessTenantQuotaProfile  `json:"tenantQuotas,omitempty"`
	PublishBackpressure  *clusterMembershipProcessPublishBackpressure `json:"publishBackpressure,omitempty"`
}

type clusterMembershipProcessPublishBackpressure struct {
	MinFreeBytes       uint64 `json:"minFreeBytes"`
	MaxCommitLagMillis uint64 `json:"maxCommitLagMillis"`
}

type clusterMembershipProcessTenantQuotaProfile struct {
	Version uint64                                      `json:"version"`
	Default clusterMembershipProcessTenantQuotaLimits   `json:"default"`
	Tenants []clusterMembershipProcessTenantQuotaTenant `json:"tenants"`
}

type clusterMembershipProcessTenantQuotaLimits struct {
	RetainedBytes        uint64 `json:"retainedBytes"`
	OfflineQueueMessages uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    uint64 `json:"offlineQueueBytes"`
	StorageBytes         uint64 `json:"storageBytes"`
}

type clusterMembershipProcessTenantQuotaTenant struct {
	TenantID             string `json:"tenantId"`
	RetainedBytes        uint64 `json:"retainedBytes"`
	OfflineQueueMessages uint64 `json:"offlineQueueMessages"`
	OfflineQueueBytes    uint64 `json:"offlineQueueBytes"`
	StorageBytes         uint64 `json:"storageBytes"`
}

type clusterMembershipProcessMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type clusterMembershipProcessTLS struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type clusterMembershipProcessManagement struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
	ReadOnlyAPIKeyFile    string `json:"readOnlyApiKeyFile,omitempty"`
	AuditDirectory        string `json:"auditDirectory"`
	RequestRate           int    `json:"requestRate"`
	AuthFailureRate       int    `json:"authFailureRate"`
}

type clusterMembershipProcessMQTT struct {
	TCPListenAddress   string `json:"tcpListenAddress"`
	TLSListenAddress   string `json:"tlsListenAddress"`
	SecurityConfigFile string `json:"securityConfigFile"`
}

type clusterMembershipProcessStageRequest struct {
	MemberID             uint64 `json:"memberId"`
	Address              string `json:"address"`
	DNSSAN               string `json:"dnsSan"`
	CertificateSHA256    string `json:"certificateSha256"`
	ReleaseVersion       string `json:"releaseVersion"`
	ClusterProtocolMajor uint32 `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32 `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32 `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32 `json:"stateSchemaMinor"`
}

type clusterMembershipProcessOperation struct {
	OperationID    string `json:"operationId"`
	Action         string `json:"action"`
	MemberID       uint64 `json:"memberId"`
	Phase          string `json:"phase"`
	CommittedIndex uint64 `json:"committedIndex"`
}

type clusterMembershipProcessOperationEnvelope struct {
	Data clusterMembershipProcessOperation `json:"data"`
}

type clusterMembershipProcessClusterSummary struct {
	ClusterID       string  `json:"clusterId"`
	ReleaseVersion  string  `json:"releaseVersion"`
	MemberCount     int     `json:"memberCount"`
	QuorumAvailable bool    `json:"quorumAvailable"`
	LeaderNodeID    *string `json:"leaderNodeId"`
}

type clusterMembershipProcessClusterEnvelope struct {
	Data clusterMembershipProcessClusterSummary `json:"data"`
}

type clusterMembershipProcessCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

type clusterMembershipProcessCertificate struct {
	certPEM     []byte
	keyPEM      []byte
	dnsName     string
	fingerprint [sha256.Size]byte
}

func (certificate clusterMembershipProcessCertificate) certPath(root string) string {
	return filepath.Join(root, "certificates", certificate.dnsName+".pem")
}

func (certificate clusterMembershipProcessCertificate) keyPath(root string) string {
	return filepath.Join(root, "certificates", certificate.dnsName+".key")
}

func newClusterMembershipProcessCA(t *testing.T) clusterMembershipProcessCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate process-test CA key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate process-test CA serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "NaruMQ dynamic membership process-test CA"},
		NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(2 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create process-test CA certificate: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse process-test CA certificate: %v", err)
	}
	return clusterMembershipProcessCA{
		cert: certificate, key: key,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func newClusterMembershipProcessCertificate(t *testing.T, ca clusterMembershipProcessCA, dnsName string) clusterMembershipProcessCertificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate process-test certificate key for %s: %v", dnsName, err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate process-test certificate serial for %s: %v", dnsName, err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(2 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{dnsName, "localhost"},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create process-test certificate for %s: %v", dnsName, err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse process-test certificate for %s: %v", dnsName, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal process-test key for %s: %v", dnsName, err)
	}
	return clusterMembershipProcessCertificate{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		dnsName: dnsName, fingerprint: sha256.Sum256(certificate.Raw),
	}
}

type clusterProcessJWTIssuer struct {
	jwksURL    string
	signingKey jwk.Key
}

func clusterMembershipProcessJWTFixture(t *testing.T) (string, string) {
	t.Helper()
	issuer := newClusterProcessJWTIssuer(t, "process-acceptance-key")
	return issuer.jwksURL, issuer.sign(t, "mqtt-process-client", time.Now().Add(time.Hour), nil)
}

func newClusterProcessJWTIssuer(t *testing.T, keyID string) *clusterProcessJWTIssuer {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate MQTT process-test signing key: %v", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		t.Fatalf("import MQTT process-test signing key: %v", err)
	}
	if err := signingKey.Set("kid", keyID); err != nil {
		t.Fatalf("set process-test signing key ID: %v", err)
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		t.Fatalf("set process-test signing algorithm: %v", err)
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		t.Fatalf("derive MQTT process-test public key: %v", err)
	}
	if err := publicKey.Set("kid", keyID); err != nil {
		t.Fatalf("set process-test public key ID: %v", err)
	}
	if err := publicKey.Set("alg", "RS256"); err != nil {
		t.Fatalf("set process-test public key algorithm: %v", err)
	}
	keySet := jwk.NewSet()
	if err := keySet.AddKey(publicKey); err != nil {
		t.Fatalf("add process-test public key to JWKS: %v", err)
	}
	keySetJSON, err := json.Marshal(keySet)
	if err != nil {
		t.Fatalf("encode process-test JWKS: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write(keySetJSON)
	}))
	t.Cleanup(server.Close)
	return &clusterProcessJWTIssuer{jwksURL: server.URL + "/jwks.json", signingKey: signingKey}
}

func (issuer *clusterProcessJWTIssuer) sign(t *testing.T, subject string, expiresAt time.Time, extraClaims map[string]any) string {
	t.Helper()
	token := jwt.New()
	for name, value := range map[string]any{
		"iss": "https://issuer.example",
		"aud": []string{"narumq"},
		"sub": subject,
		"exp": expiresAt,
	} {
		if err := token.Set(name, value); err != nil {
			t.Fatalf("set process-test JWT claim %q: %v", name, err)
		}
	}
	for name, value := range extraClaims {
		if err := token.Set(name, value); err != nil {
			t.Fatalf("set process-test JWT extra claim %q: %v", name, err)
		}
	}
	signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), issuer.signingKey))
	if err != nil {
		t.Fatalf("sign MQTT process-test JWT: %v", err)
	}
	return string(signedToken)
}

func clusterMembershipProcessRepoRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate process acceptance test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repository root from test source %q has no go.mod: %v", root, err)
	}
	return root
}

func clusterMembershipProcessAddresses(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	addresses := make([]string, 0, count)
	t.Cleanup(func() {
		clusterMembershipProcessAddressLeases.Lock()
		defer clusterMembershipProcessAddressLeases.Unlock()
		for _, address := range addresses {
			delete(clusterMembershipProcessAddressLeases.addresses, address)
		}
	})
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	for len(addresses) < count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve loopback port: %v", err)
		}
		address := listener.Addr().String()
		clusterMembershipProcessAddressLeases.Lock()
		_, leased := clusterMembershipProcessAddressLeases.addresses[address]
		if !leased {
			clusterMembershipProcessAddressLeases.addresses[address] = struct{}{}
		}
		clusterMembershipProcessAddressLeases.Unlock()
		if leased {
			_ = listener.Close()
			continue
		}
		listeners = append(listeners, listener)
		addresses = append(addresses, address)
	}
	return addresses
}

func clusterMembershipProcessPort(address string) string {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		panic(fmt.Sprintf("invalid process-test address %q: %v", address, err))
	}
	return port
}

func clusterMembershipProcessWriteJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("encode generated config %q: %v", path, err)
	}
	clusterMembershipProcessWriteFile(t, path, data, 0o600)
}

func clusterMembershipProcessWriteFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create directory for %q: %v", path, err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}

func clusterMembershipProcessWaitForStatus(t *testing.T, ctx context.Context, process *clusterMembershipProcess, path string, want int) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, process.name+" "+path, func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+path, nil, "", "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != want {
			return false, fmt.Sprintf("status=%d body=%s want=%d", status, body, want), nil
		}
		return true, "", nil
	})
}

func clusterMembershipProcessWaitForBootstrap(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess) {
	t.Helper()
	clusterMembershipProcessWaitForBootstrapRelease(t, ctx, processes, clusterMembershipProcessRelease)
}

func clusterMembershipProcessWaitForBootstrapRelease(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, release string) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, "three bootstrap voters ready with an elected leader", func() (bool, string, error) {
		for _, process := range processes {
			if process == nil {
				return false, "not all bootstrap processes have started", nil
			}
			if err := process.isAlive(); err != nil {
				return false, "", err
			}
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
			if err != nil {
				return false, process.name + " readiness request: " + err.Error(), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s readiness status=%d body=%s", process.name, status, body), nil
			}
		}
		leader := ""
		for _, process := range processes {
			summary, status, body, err := clusterMembershipProcessGetSummary(ctx, process.managementURL(), clusterMembershipProcessAdmin)
			if err != nil {
				return false, process.name + " cluster summary: " + err.Error(), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s cluster summary status=%d body=%s", process.name, status, body), nil
			}
			if summary.ClusterID != clusterMembershipProcessClusterID || summary.ReleaseVersion != release || summary.MemberCount != 3 || !summary.QuorumAvailable || summary.LeaderNodeID == nil || *summary.LeaderNodeID == "" {
				return false, fmt.Sprintf("%s cluster summary=%+v, want release %s, 3 voters, quorum, and leader", process.name, summary, release), nil
			}
			if leader == "" {
				leader = *summary.LeaderNodeID
			} else if leader != *summary.LeaderNodeID {
				return false, fmt.Sprintf("bootstrap leader views disagree: %s reports %s, %s reports %s", processes[0].name, leader, process.name, *summary.LeaderNodeID), nil
			}
		}
		return true, "", nil
	})
}

func clusterMembershipProcessFindLeader(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, expectedCount int) (uint64, string) {
	t.Helper()
	for _, process := range processes {
		summary, status, body, err := clusterMembershipProcessGetSummary(ctx, process.managementURL(), clusterMembershipProcessAdmin)
		if err != nil || status != http.StatusOK || summary.LeaderNodeID == nil {
			continue
		}
		if summary.MemberCount != expectedCount {
			t.Fatalf("cluster summary on %s has %d members (status=%d body=%s), want %d", process.name, summary.MemberCount, status, body, expectedCount)
		}
		id, parseErr := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
		if parseErr == nil && id > 0 && id <= uint64(len(processes)) {
			return id, processes[id-1].managementURL()
		}
	}
	t.Fatalf("no bootstrap node reported a numeric cluster leader")
	return 0, ""
}

func clusterMembershipProcessWaitForOperation(t *testing.T, ctx context.Context, process *clusterMembershipProcess, operationID, phase string) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, "membership operation "+operationID+" to reach "+phase, func() (bool, string, error) {
		if err := process.isAlive(); err != nil {
			return false, "", err
		}
		status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
			process.managementURL()+"/api/v1/cluster/membership-operations/"+operationID, nil, clusterMembershipProcessAdmin, "")
		if err != nil {
			return false, err.Error(), nil
		}
		if status != http.StatusOK {
			return false, fmt.Sprintf("operation status=%d body=%s", status, body), nil
		}
		operation, decodeErr := clusterMembershipProcessDecodeOperation(body)
		if decodeErr != nil {
			return false, fmt.Sprintf("decode operation response: %v body=%s", decodeErr, body), nil
		}
		if operation.OperationID != operationID {
			return false, fmt.Sprintf("returned operation ID=%q", operation.OperationID), nil
		}
		if operation.Phase == phase {
			return true, "", nil
		}
		return false, "phase=" + operation.Phase, nil
	})
}

func clusterMembershipProcessWaitForClusterSize(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, want int) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("cluster member count %d", want), func() (bool, string, error) {
		for _, process := range processes {
			if err := process.isAlive(); err != nil {
				return false, "", err
			}
			readyStatus, readyBody, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
			if err != nil {
				return false, process.name + " readiness request: " + err.Error(), nil
			}
			if readyStatus != http.StatusOK {
				return false, fmt.Sprintf("%s readiness status=%d body=%s", process.name, readyStatus, readyBody), nil
			}
			summary, status, body, err := clusterMembershipProcessGetSummary(ctx, process.managementURL(), clusterMembershipProcessAdmin)
			if err != nil {
				return false, process.name + " summary request: " + err.Error(), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s summary status=%d body=%s", process.name, status, body), nil
			}
			if summary.MemberCount != want || summary.LeaderNodeID == nil || !summary.QuorumAvailable {
				return false, fmt.Sprintf("%s summary=%+v", process.name, summary), nil
			}
		}
		return true, "", nil
	})
}

func clusterMembershipProcessAssertConvergedSummary(t *testing.T, ctx context.Context, bootstrap []*clusterMembershipProcess, joiner *clusterMembershipProcess, expectedCount int) {
	t.Helper()
	all := append(append([]*clusterMembershipProcess(nil), bootstrap...), joiner)
	clusterMembershipProcessWait(t, ctx, "converged cluster summary across all four processes", func() (bool, string, error) {
		leader := ""
		for _, process := range all {
			if err := process.isAlive(); err != nil {
				return false, "", err
			}
			readyStatus, readyBody, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/health/ready", nil, "", "")
			if err != nil {
				return false, process.name + " readiness request: " + err.Error(), nil
			}
			if readyStatus != http.StatusOK {
				return false, fmt.Sprintf("%s readiness status=%d body=%s", process.name, readyStatus, readyBody), nil
			}
			summary, status, body, err := clusterMembershipProcessGetSummary(ctx, process.managementURL(), clusterMembershipProcessAdmin)
			if err != nil {
				return false, process.name + " summary request: " + err.Error(), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s summary status=%d body=%s", process.name, status, body), nil
			}
			if summary.MemberCount != expectedCount || !summary.QuorumAvailable || summary.LeaderNodeID == nil || *summary.LeaderNodeID == "" {
				return false, fmt.Sprintf("%s cluster summary=%+v, want %d members, quorum, and leader", process.name, summary, expectedCount), nil
			}
			if leader == "" {
				leader = *summary.LeaderNodeID
			} else if leader != *summary.LeaderNodeID {
				return false, fmt.Sprintf("cluster leader views disagree: %s reports %s, %s reports %s", all[0].name, leader, process.name, *summary.LeaderNodeID), nil
			}
		}
		return true, "", nil
	})
}

func clusterMembershipProcessGetSummary(ctx context.Context, baseURL, token string) (clusterMembershipProcessClusterSummary, int, string, error) {
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, baseURL+"/api/v1/cluster", nil, token, "")
	if err != nil {
		return clusterMembershipProcessClusterSummary{}, status, string(body), err
	}
	var envelope clusterMembershipProcessClusterEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return clusterMembershipProcessClusterSummary{}, status, string(body), fmt.Errorf("decode cluster response: %w", err)
	}
	return envelope.Data, status, string(body), nil
}

func clusterMembershipProcessDecodeOperation(body []byte) (clusterMembershipProcessOperation, error) {
	var envelope clusterMembershipProcessOperationEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return clusterMembershipProcessOperation{}, err
	}
	if envelope.Data.OperationID == "" {
		return clusterMembershipProcessOperation{}, errors.New("operationId is empty")
	}
	return envelope.Data, nil
}

func clusterMembershipProcessRequest(ctx context.Context, method, url string, body []byte, token, idempotencyKey string) (int, []byte, error) {
	header := make(http.Header)
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	if idempotencyKey != "" {
		header.Set("Idempotency-Key", idempotencyKey)
	}
	return clusterMembershipProcessRequestWithHeader(ctx, method, url, body, header)
}

func clusterMembershipProcessRequestWithHeader(ctx context.Context, method, url string, body []byte, header http.Header) (int, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(requestCtx, method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	for name, values := range header {
		request.Header[name] = append([]string(nil), values...)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	// Child processes reuse listener addresses after restart; do not reuse pooled connections across process lifetimes.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Timeout: 5 * time.Second, Transport: transport}).Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return response.StatusCode, nil, err
	}
	return response.StatusCode, responseBody, nil
}

func clusterMembershipProcessWait(t *testing.T, ctx context.Context, description string, probe func() (bool, string, error)) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var last string
	for {
		complete, detail, err := probe()
		if err != nil {
			t.Fatalf("%s: %v", description, err)
		}
		if complete {
			return
		}
		last = detail
		select {
		case <-waitCtx.Done():
			t.Fatalf("timed out waiting for %s: %v; last observation: %s", description, waitCtx.Err(), last)
		case <-ticker.C:
		}
	}
}

func clusterMembershipProcessAssertMQTTRejected(t *testing.T, address string) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("connect to pre-readiness MQTT listener: %v", err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	clientID := "pre-readiness-client"
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 60, 0, byte(len(clientID))}
	body = append(body, clientID...)
	packet := []byte{0x10, byte(len(body))}
	packet = append(packet, body...)
	if _, err := connection.Write(packet); err != nil {
		if clusterMembershipProcessIsClosed(err) {
			return
		}
		t.Fatalf("send valid MQTT CONNECT before readiness: %v", err)
	}
	var header [2]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		if clusterMembershipProcessIsClosed(err) {
			return
		}
		t.Fatalf("read pre-readiness MQTT response: %v", err)
	}
	if header[0] != 0x20 || header[1] != 2 {
		t.Fatalf("pre-readiness MQTT response header=%x, want a two-byte CONNACK or connection close", header)
	}
	var bodyBytes [2]byte
	if _, err := io.ReadFull(connection, bodyBytes[:]); err != nil {
		if clusterMembershipProcessIsClosed(err) {
			return
		}
		t.Fatalf("read pre-readiness MQTT CONNACK body: %v", err)
	}
	if bodyBytes[1] == 0 {
		t.Fatalf("pre-readiness MQTT CONNECT received positive CONNACK: %x %x", header, bodyBytes)
	}
}
func clusterMembershipProcessAssertMQTTAccepted(t *testing.T, address, accessToken string) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("connect to ready MQTT listener: %v", err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))

	clientID := "ready-connection-client"
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xC2, 0, 60}
	for _, field := range []string{clientID, "jwt", accessToken} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	packet := []byte{0x10}
	remaining := len(body)
	for {
		digit := byte(remaining % 128)
		remaining /= 128
		if remaining > 0 {
			digit |= 0x80
		}
		packet = append(packet, digit)
		if remaining == 0 {
			break
		}
	}
	packet = append(packet, body...)
	if _, err := connection.Write(packet); err != nil {
		t.Fatalf("send MQTT CONNECT after readiness: %v", err)
	}
	var header [2]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		t.Fatalf("read post-readiness MQTT CONNACK header: %v", err)
	}
	var response [2]byte
	if _, err := io.ReadFull(connection, response[:]); err != nil {
		t.Fatalf("read post-readiness MQTT CONNACK body: %v", err)
	}
	if header != [2]byte{0x20, 2} || response != [2]byte{0, 0} {
		t.Fatalf("post-readiness MQTT CONNACK = %x %x, want accepted 3.1.1 CONNACK", header, response)
	}
	if _, err := connection.Write([]byte{0xE0, 0}); err != nil && !clusterMembershipProcessIsClosed(err) {
		t.Fatalf("send MQTT DISCONNECT after readiness proof: %v", err)
	}
}

func clusterMembershipProcessIsClosed(err error) bool {
	var networkError *net.OpError
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) ||
		(errors.As(err, &networkError) && !networkError.Timeout())
}

func clusterMembershipProcessRequireFailpointExit(t *testing.T, process *clusterMembershipProcess, point string) {
	t.Helper()
	select {
	case <-process.done:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not exit waiting for failpoint %s", process.name, point)
	}
	code, ok := process.exitCode()
	if !ok || code != 86 {
		t.Fatalf("%s exit code = %d (ok=%t), want 86; logs:\n%s", process.name, code, ok, process.logs.String())
	}
	wantMarker := "narumq failpoint reached: " + point
	if !strings.Contains(process.logs.String(), wantMarker) {
		t.Fatalf("%s output missing marker %q; logs:\n%s", process.name, wantMarker, process.logs.String())
	}
}

func clusterMembershipProcessOpenSubscribedSession(t *testing.T, address, accessToken, clientID, filter string) net.Conn {
	return clusterMembershipProcessOpenSubscribedSessionWithClean(t, address, accessToken, clientID, filter, false)
}

func clusterMembershipProcessOpenCleanSubscribedSession(t *testing.T, address, accessToken, clientID, filter string) net.Conn {
	return clusterMembershipProcessOpenSubscribedSessionWithClean(t, address, accessToken, clientID, filter, true)
}

func clusterMembershipProcessOpenSubscribedSessionWithClean(t *testing.T, address, accessToken, clientID, filter string, cleanSession bool) net.Conn {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := clusterMembershipProcessTryOpenSubscribedSessionWithClean(address, accessToken, clientID, filter, cleanSession)
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("open subscribed session to %s timed out: %v", address, lastErr)
	return nil
}

func clusterMembershipProcessTryOpenSubscribedSession(address, accessToken, clientID, filter string) (net.Conn, error) {
	return clusterMembershipProcessTryOpenSubscribedSessionWithClean(address, accessToken, clientID, filter, false)
}

func clusterMembershipProcessTryOpenSubscribedSessionWithClean(address, accessToken, clientID, filter string, cleanSession bool) (net.Conn, error) {
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))

	// CONNECT packet (MQTT 3.1.1, username + password; clean-session is selected by the caller).
	connectFlags := byte(0xC0)
	if cleanSession {
		connectFlags |= 0x02
	}
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, connectFlags, 0, 60}
	for _, field := range []string{clientID, "jwt", accessToken} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	packet := []byte{0x10}
	remaining := len(body)
	for {
		digit := byte(remaining % 128)
		remaining /= 128
		if remaining > 0 {
			digit |= 0x80
		}
		packet = append(packet, digit)
		if remaining == 0 {
			break
		}
	}
	packet = append(packet, body...)
	if _, err := connection.Write(packet); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("send CONNECT: %w", err)
	}
	var connackHeader [2]byte
	if _, err := io.ReadFull(connection, connackHeader[:]); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("read CONNACK header: %w", err)
	}
	var connackBody [2]byte
	if _, err := io.ReadFull(connection, connackBody[:]); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("read CONNACK body: %w", err)
	}
	if connackHeader != [2]byte{0x20, 2} || connackBody[1] != 0 {
		_ = connection.Close()
		return nil, fmt.Errorf("unexpected CONNACK: %x %x", connackHeader, connackBody)
	}

	// SUBSCRIBE packet: 0x82, packet ID 1, filter, QoS 0
	subPayload := []byte{0, 1, byte(len(filter) >> 8), byte(len(filter))}
	subPayload = append(subPayload, filter...)
	subPayload = append(subPayload, 0)
	subPacket := []byte{0x82, byte(len(subPayload))}
	subPacket = append(subPacket, subPayload...)
	if _, err := connection.Write(subPacket); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("send SUBSCRIBE: %w", err)
	}
	var suback [5]byte
	if _, err := io.ReadFull(connection, suback[:]); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("read SUBACK: %w", err)
	}
	if suback != [5]byte{0x90, 0x03, 0x00, 0x01, 0x00} {
		_ = connection.Close()
		return nil, fmt.Errorf("unexpected SUBACK: %x, want 9003000100", suback)
	}

	_ = connection.SetDeadline(time.Time{})
	return connection, nil
}

func clusterMembershipProcessReconnectSession(t *testing.T, address, accessToken, clientID string) (net.Conn, bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, present, err := clusterMembershipProcessTryReconnectSession(address, accessToken, clientID)
		if err == nil {
			return conn, present
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reconnect session to %s timed out: %v", address, lastErr)
	return nil, false
}

func clusterMembershipProcessTryReconnectSession(address, accessToken, clientID string) (net.Conn, bool, error) {
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return nil, false, fmt.Errorf("dial: %w", err)
	}
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))

	// CONNECT packet (MQTT 3.1.1, clean session 0, username + password => flags 0xC0)
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xC0, 0, 60}
	for _, field := range []string{clientID, "jwt", accessToken} {
		body = append(body, byte(len(field)>>8), byte(len(field)))
		body = append(body, field...)
	}
	packet := clusterMembershipProcessEncodeMQTTPacket(0x10, body)
	if _, err := connection.Write(packet); err != nil {
		_ = connection.Close()
		return nil, false, fmt.Errorf("send CONNECT: %w", err)
	}
	var connackHeader [2]byte
	if _, err := io.ReadFull(connection, connackHeader[:]); err != nil {
		_ = connection.Close()
		return nil, false, fmt.Errorf("read CONNACK header: %w", err)
	}
	var connackBody [2]byte
	if _, err := io.ReadFull(connection, connackBody[:]); err != nil {
		_ = connection.Close()
		return nil, false, fmt.Errorf("read CONNACK body: %w", err)
	}
	if connackHeader != [2]byte{0x20, 2} || connackBody[1] != 0 {
		_ = connection.Close()
		return nil, false, fmt.Errorf("unexpected CONNACK: %x %x", connackHeader, connackBody)
	}
	_ = connection.SetDeadline(time.Time{})
	return connection, (connackBody[0] & 1) != 0, nil
}

func clusterMembershipProcessAssertConnectionClosed(t *testing.T, conn net.Conn, timeout time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var buf [1]byte
	_, err := conn.Read(buf[:])
	if err == nil {
		t.Fatal("expected connection to be closed, but read succeeded")
	}
	if !clusterMembershipProcessIsClosed(err) {
		t.Fatalf("expected closed error, got: %v", err)
	}
}

func clusterMembershipProcessAssertConnectionOpen(t *testing.T, conn net.Conn, wait time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(wait))
	var buf [1]byte
	_, err := conn.Read(buf[:])
	if err == nil {
		t.Fatal("unexpected read on open idle connection")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout indicating open connection, got: %v", err)
	}
	_ = conn.SetDeadline(time.Time{})
}

func clusterMembershipProcessWaitForRouteProjection(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, wantFilter string) {
	t.Helper()
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("route projection for filter %q", wantFilter), func() (bool, string, error) {
		type subTuple struct {
			tenantID string
			clientID string
			filter   string
			qos      int
		}
		var reference *subTuple
		for _, process := range processes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, process.managementURL()+"/api/v1/subscriptions?tenantId=acceptance", nil, clusterMembershipProcessAdmin, "")
			if err != nil {
				return false, fmt.Sprintf("%s query error: %v", process.name, err), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s status = %d", process.name, status), nil
			}
			var list struct {
				Data []struct {
					TenantID    string `json:"tenantId"`
					ClientID    string `json:"clientId"`
					TopicFilter string `json:"topicFilter"`
					QoS         int    `json:"qos"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(body), &list); err != nil {
				return false, fmt.Sprintf("%s decode error: %v", process.name, err), nil
			}
			if len(list.Data) != 1 {
				return false, fmt.Sprintf("%s has %d subscriptions, want 1", process.name, len(list.Data)), nil
			}
			sub := list.Data[0]
			if sub.TenantID != "acceptance" || sub.TopicFilter != wantFilter {
				return false, fmt.Sprintf("%s subscription = %+v, want tenant=acceptance, filter=%s", process.name, sub, wantFilter), nil
			}
			tuple := subTuple{
				tenantID: sub.TenantID,
				clientID: sub.ClientID,
				filter:   sub.TopicFilter,
				qos:      sub.QoS,
			}
			if reference == nil {
				reference = &tuple
			} else if *reference != tuple {
				return false, fmt.Sprintf("projection mismatch between nodes: reference=%+v, %s=%+v", *reference, process.name, tuple), nil
			}
		}
		return true, "all nodes agree on route projection", nil
	})
}

func clusterMembershipProcessWaitForRouteProjectionClients(
	t *testing.T,
	ctx context.Context,
	processes []*clusterMembershipProcess,
	wantFilter string,
	wantClientIDs ...string,
) {
	t.Helper()
	want := append([]string(nil), wantClientIDs...)
	sort.Strings(want)
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("route projection for filter %q and clients %v", wantFilter, want), func() (bool, string, error) {
		var reference []string
		for _, process := range processes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				process.managementURL()+"/api/v1/subscriptions?tenantId=acceptance", nil, clusterMembershipProcessAdmin, "")
			if err != nil {
				return false, fmt.Sprintf("%s query error: %v", process.name, err), nil
			}
			if status != http.StatusOK {
				return false, fmt.Sprintf("%s status = %d", process.name, status), nil
			}
			var list struct {
				Data []struct {
					TenantID    string `json:"tenantId"`
					ClientID    string `json:"clientId"`
					TopicFilter string `json:"topicFilter"`
					QoS         int    `json:"qos"`
				} `json:"data"`
			}
			if err := json.Unmarshal([]byte(body), &list); err != nil {
				return false, fmt.Sprintf("%s decode error: %v", process.name, err), nil
			}
			if len(list.Data) != len(want) {
				return false, fmt.Sprintf("%s has %d subscriptions, want %d", process.name, len(list.Data), len(want)), nil
			}
			clientIDs := make([]string, len(list.Data))
			for index, subscription := range list.Data {
				if subscription.TenantID != "acceptance" || subscription.TopicFilter != wantFilter || subscription.QoS != 0 {
					return false, fmt.Sprintf("%s subscription = %+v, want tenant=acceptance filter=%s QoS=0", process.name, subscription, wantFilter), nil
				}
				clientIDs[index] = subscription.ClientID
			}
			sort.Strings(clientIDs)
			if !slices.Equal(clientIDs, want) {
				return false, fmt.Sprintf("%s clients = %v, want %v", process.name, clientIDs, want), nil
			}
			if reference == nil {
				reference = clientIDs
			} else if !slices.Equal(reference, clientIDs) {
				return false, fmt.Sprintf("projection mismatch: reference=%v, %s=%v", reference, process.name, clientIDs), nil
			}
		}
		return true, "all nodes agree on route projection", nil
	})
}

func clusterMembershipProcessRequireLivePublish(t *testing.T, connection net.Conn, topic, payload string) {
	t.Helper()
	publishes := clusterMembershipProcessReadPublishes(t, connection, 1)
	if publishes[0] != [2]string{topic, payload} {
		t.Fatalf("live publishes = %v, want topic=%q payload=%q", publishes, topic, payload)
	}
}

func clusterMembershipProcessPublish(t *testing.T, address, accessToken, clientID, topic string, payload []byte) {
	t.Helper()
	clusterMembershipProcessPublishWithRetain(t, address, accessToken, clientID, topic, payload, false)
}

func clusterMembershipProcessPublishRetained(t *testing.T, address, accessToken, clientID, topic string, payload []byte) {
	t.Helper()
	clusterMembershipProcessPublishWithRetain(t, address, accessToken, clientID, topic, payload, true)
}

func clusterMembershipProcessPublishWithRetain(t *testing.T, address, accessToken, clientID, topic string, payload []byte, retain bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		err := clusterMembershipProcessTryPublishWithRetain(address, accessToken, clientID, topic, payload, retain)
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("publish to %s timed out: %v", address, lastErr)
}

func clusterMembershipProcessTryPublishWithRetain(address, accessToken, clientID, topic string, payload []byte, retain bool) error {
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))

	connectBody := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xC2, 0, 60}
	for _, field := range []string{clientID, "jwt", accessToken} {
		connectBody = append(connectBody, byte(len(field)>>8), byte(len(field)))
		connectBody = append(connectBody, field...)
	}
	if _, err := connection.Write(clusterMembershipProcessEncodeMQTTPacket(0x10, connectBody)); err != nil {
		return fmt.Errorf("send CONNECT: %w", err)
	}
	header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil || header != 0x20 || !bytes.Equal(body, []byte{0, 0}) {
		return fmt.Errorf("publisher CONNACK = (%x, %x), error %v", header, body, err)
	}

	if len(topic) > 0xffff {
		return fmt.Errorf("topic length %d exceeds MQTT string limit", len(topic))
	}
	publishBody := []byte{byte(len(topic) >> 8), byte(len(topic))}
	publishBody = append(publishBody, topic...)
	publishBody = append(publishBody, payload...)
	publishHeader := byte(0x30)
	if retain {
		publishHeader = 0x31
	}
	if _, err := connection.Write(clusterMembershipProcessEncodeMQTTPacket(publishHeader, publishBody)); err != nil {
		return fmt.Errorf("send PUBLISH %q: %w", topic, err)
	}
	if _, err := connection.Write([]byte{0xC0, 0}); err != nil {
		return fmt.Errorf("send PINGREQ: %w", err)
	}
	header, body, err = clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil || header != 0xD0 || len(body) != 0 {
		return fmt.Errorf("publisher PINGRESP = (%x, %x), error %v", header, body, err)
	}
	if _, err := connection.Write([]byte{0xE0, 0}); err != nil && !clusterMembershipProcessIsClosed(err) {
		return fmt.Errorf("send DISCONNECT: %w", err)
	}
	return nil
}

func clusterMembershipProcessReadRetained(t *testing.T, connection net.Conn, want int) map[string]string {
	t.Helper()
	values := make(map[string]string, want)
	for len(values) < want {
		_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
		header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
		if err != nil {
			t.Fatalf("read retained PUBLISH %d/%d: %v", len(values)+1, want, err)
		}
		if header != 0x31 || len(body) < 2 {
			t.Fatalf("retained PUBLISH header/body = %02x/%x, want QoS 0 RETAIN", header, body)
		}
		topicLength := int(binary.BigEndian.Uint16(body[:2]))
		if topicLength == 0 || topicLength > len(body)-2 {
			t.Fatalf("retained PUBLISH topic length %d exceeds body length %d", topicLength, len(body))
		}
		topic := string(body[2 : 2+topicLength])
		if _, exists := values[topic]; exists {
			t.Fatalf("duplicate retained PUBLISH for topic %q", topic)
		}
		values[topic] = string(body[2+topicLength:])
	}

	_ = connection.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if header, body, err := clusterMembershipProcessReadMQTTPacket(connection); err == nil {
		t.Fatalf("unexpected MQTT packet after retained replay: header=%02x body=%x", header, body)
	} else {
		var networkError net.Error
		if !errors.As(err, &networkError) || !networkError.Timeout() {
			t.Fatalf("retained replay connection ended instead of remaining idle: %v", err)
		}
	}
	_ = connection.SetReadDeadline(time.Time{})
	return values
}

func clusterMembershipProcessReadPublishes(t *testing.T, connection net.Conn, want int) [][2]string {
	t.Helper()
	var values [][2]string
	for len(values) < want {
		_ = connection.SetReadDeadline(time.Now().Add(5 * time.Second))
		header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
		if err != nil {
			t.Fatalf("read PUBLISH %d/%d: %v", len(values)+1, want, err)
		}
		if header != 0x30 || len(body) < 2 {
			t.Fatalf("PUBLISH header/body = %02x/%x, want QoS 0, retain 0", header, body)
		}
		topicLength := int(binary.BigEndian.Uint16(body[:2]))
		if topicLength == 0 || topicLength > len(body)-2 {
			t.Fatalf("PUBLISH topic length %d exceeds body length %d", topicLength, len(body))
		}
		topic := string(body[2 : 2+topicLength])
		payload := string(body[2+topicLength:])
		values = append(values, [2]string{topic, payload})
	}

	_ = connection.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if header, body, err := clusterMembershipProcessReadMQTTPacket(connection); err == nil {
		t.Fatalf("unexpected MQTT packet after publishes: header=%02x body=%x", header, body)
	} else {
		var networkError net.Error
		if !errors.As(err, &networkError) || !networkError.Timeout() {
			t.Fatalf("connection ended instead of remaining idle: %v", err)
		}
	}
	_ = connection.SetReadDeadline(time.Time{})
	return values
}

func clusterMembershipProcessEncodeMQTTPacket(header byte, body []byte) []byte {
	packet := []byte{header}
	remaining := len(body)
	for {
		digit := byte(remaining % 128)
		remaining /= 128
		if remaining > 0 {
			digit |= 0x80
		}
		packet = append(packet, digit)
		if remaining == 0 {
			break
		}
	}
	return append(packet, body...)
}

func clusterMembershipProcessReadMQTTPacket(connection net.Conn) (byte, []byte, error) {
	var header [1]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return 0, nil, err
	}
	remaining, multiplier, complete := 0, 1, false
	for range 4 {
		var encoded [1]byte
		if _, err := io.ReadFull(connection, encoded[:]); err != nil {
			return 0, nil, err
		}
		remaining += int(encoded[0]&0x7f) * multiplier
		if encoded[0]&0x80 == 0 {
			complete = true
			break
		}
		multiplier *= 128
	}
	if !complete {
		return 0, nil, errors.New("MQTT remaining length uses more than four bytes")
	}
	body := make([]byte, remaining)
	if _, err := io.ReadFull(connection, body); err != nil {
		return 0, nil, err
	}
	return header[0], body, nil
}
