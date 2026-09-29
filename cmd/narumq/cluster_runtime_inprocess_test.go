package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"narumq/internal/adapters/raft/records"
)

func TestRunClusterModeInProcess(t *testing.T) {
	directory := t.TempDir()
	caCert, caKey, caPEM := newClusterRuntimeTestCA(t)
	caPath := filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	peerAddresses := []string{clusterRuntimeTestFreeAddress(t), clusterRuntimeTestFreeAddress(t), clusterRuntimeTestFreeAddress(t)}
	managementAddresses := []string{clusterRuntimeTestFreeAddress(t), clusterRuntimeTestFreeAddress(t), clusterRuntimeTestFreeAddress(t)}
	bootstrapMembers := make([]clusterBootstrapMember, 3)
	configs := make([]clusterConfigFile, 3)
	for index := range 3 {
		nodeID := uint64(index + 1)
		dnsName := fmt.Sprintf("node%d.example.test", nodeID)
		certPEM, keyPEM, fingerprint := issueClusterRuntimeTestCertificate(t, caCert, caKey, dnsName, int64(nodeID+1))
		certPath := filepath.Join(directory, fmt.Sprintf("node%d.crt", nodeID))
		keyPath := filepath.Join(directory, fmt.Sprintf("node%d.key", nodeID))
		if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		bootstrapMembers[index] = clusterBootstrapMember{
			MemberID: nodeID, Address: peerAddresses[index], DNSSAN: dnsName,
			CertificateSHA256: hex.EncodeToString(fingerprint[:]),
		}
		configs[index] = clusterConfigFile{
			Mode: "cluster", ClusterID: "inprocess-coverage", NodeID: nodeID,
			Directory:     filepath.Join(directory, fmt.Sprintf("node%d", nodeID)),
			ListenAddress: peerAddresses[index], AdvertisedAddress: peerAddresses[index],
			LocalDNSName: dnsName, ReleaseVersion: buildReleaseVersion,
			ClusterProtocolMajor: 2, StateSchemaMajor: 1, Role: "bootstrap",
			PeerTLS: clusterPeerTLS{CertFile: certPath, KeyFile: keyPath, CAFile: caPath},
			Management: clusterManagement{
				ListenAddress:         managementAddresses[index],
				ClusterAdminTokenFile: filepath.Join(directory, "admin-token"),
			},
			MQTT: clusterMQTT{SecurityConfigFile: filepath.Join(directory, "security.json")},
		}
	}
	for index := range configs {
		configs[index].BootstrapMembers = bootstrapMembers
	}

	tokenPath := configs[0].Management.ClusterAdminTokenFile
	if err := os.WriteFile(tokenPath, []byte("admin-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	securityPath := configs[0].MQTT.SecurityConfigFile
	securityJSON := `{
  "jwt": {
    "issuer": "https://issuer.example",
    "audience": "narumq",
    "algorithms": ["RS256"],
    "jwksUrl": "https://issuer.example/.well-known/jwks.json",
    "cacheTtl": "15m",
    "refreshCooldown": "30s",
    "clockSkew": "30s",
    "httpTimeout": "5s"
  },
  "identities": [{
    "issuer": "https://issuer.example",
    "subject": "device-1",
    "tenantId": "tenant-a",
    "credentialId": "device-1-key",
    "allowedClientIds": ["device-1"],
    "roles": ["publisher"]
  }],
  "acl": []
}`
	if err := os.WriteFile(securityPath, []byte(securityJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type nodeResult struct {
		id  uint64
		err error
	}
	results := make(chan nodeResult, len(configs)+1)
	receivedResults := 0
	expectedNodes := len(configs)
	for _, config := range configs {
		config := config
		go func() {
			results <- nodeResult{id: config.NodeID, err: runClusterMode(ctx, config, slog.New(slog.NewTextHandler(io.Discard, nil)))}
		}()
	}
	defer func() {
		cancel()
		for receivedResults < expectedNodes {
			select {
			case result := <-results:
				receivedResults++
				if result.err != nil && !errors.Is(result.err, context.Canceled) {
					t.Errorf("node %d stopped with error: %v", result.id, result.err)
				}
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for in-process node shutdown")
				return
			}
		}
	}()

	// Keep readiness probes independent across the membership transition.
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	deadline := time.Now().Add(20 * time.Second)
	lastStatuses := make([]string, len(managementAddresses))
	for time.Now().Before(deadline) {
		ready := true
		for index, address := range managementAddresses {
			response, err := client.Get("http://" + address + "/health/ready")
			if err != nil {
				lastStatuses[index] = err.Error()
				ready = false
				continue
			}
			lastStatuses[index] = response.Status
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				ready = false
			}
		}
		if ready {
			break
		}
		select {
		case result := <-results:
			receivedResults++
			t.Fatalf("node %d exited before cluster readiness: %v", result.id, result.err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	for index, status := range lastStatuses {
		if status != fmt.Sprintf("%d %s", http.StatusOK, http.StatusText(http.StatusOK)) {
			t.Fatalf("node %d readiness status = %q; all node statuses = %s", index+1, status, strings.Join(lastStatuses, ", "))
		}
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+managementAddresses[0]+"/api/v1/cluster", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer admin-token")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("query in-process cluster summary: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("cluster summary status = %d, want 200", response.StatusCode)
	}
	clusterRuntimeTestAssertNodeListing(t, ctx, client, managementAddresses[0])
	initialLeader := clusterRuntimeTestLeader(t, ctx, client, managementAddresses)
	initialBackup := clusterRuntimeTestReadBackup(t, ctx, client, initialLeader)
	if initialBackup.Manifest.GetClusterId() != "inprocess-coverage" || len(initialBackup.Manifest.GetVoterIds()) != 3 {
		t.Fatalf("initial backup manifest=%+v", initialBackup.Manifest)
	}
	if initialBackup.Manifest.GetSnapshotIndex() == 0 || initialBackup.Manifest.GetSnapshotIndex() > initialBackup.Manifest.GetCommitIndex() {
		t.Fatalf("initial backup has invalid snapshot/commit boundary: %+v", initialBackup.Manifest)
	}
	joinerID := uint64(4)
	joinerDNS := fmt.Sprintf("node%d.example.test", joinerID)
	joinerPeerAddress := clusterRuntimeTestFreeAddress(t)
	joinerManagementAddress := clusterRuntimeTestFreeAddress(t)
	joinerCertPEM, joinerKeyPEM, joinerFingerprint := issueClusterRuntimeTestCertificate(t, caCert, caKey, joinerDNS, 5)
	joinerCertPath := filepath.Join(directory, "node4.crt")
	joinerKeyPath := filepath.Join(directory, "node4.key")
	if err := os.WriteFile(joinerCertPath, joinerCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(joinerKeyPath, joinerKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	joinerConfig := clusterConfigFile{
		Mode: "cluster", ClusterID: "inprocess-coverage", NodeID: joinerID,
		Directory: filepath.Join(directory, "node4"), ListenAddress: joinerPeerAddress,
		AdvertisedAddress: joinerPeerAddress, LocalDNSName: joinerDNS, ReleaseVersion: buildReleaseVersion,
		ClusterProtocolMajor: 2, StateSchemaMajor: 1, Role: "joiner",
		SeedAddresses: []string{
			clusterRuntimeTestSeedAddress(t, peerAddresses[0]),
			clusterRuntimeTestSeedAddress(t, peerAddresses[1]),
			clusterRuntimeTestSeedAddress(t, peerAddresses[2]),
		},
		PeerTLS: clusterPeerTLS{CertFile: joinerCertPath, KeyFile: joinerKeyPath, CAFile: caPath},
		Management: clusterManagement{
			ListenAddress:         joinerManagementAddress,
			ClusterAdminTokenFile: tokenPath,
		},
		MQTT: clusterMQTT{SecurityConfigFile: securityPath},
	}
	leaderAddress := clusterRuntimeTestLeader(t, ctx, client, managementAddresses)
	stageBody, err := json.Marshal(struct {
		MemberID             uint64 `json:"memberId"`
		Address              string `json:"address"`
		DNSSAN               string `json:"dnsSan"`
		CertificateSHA256    string `json:"certificateSha256"`
		ReleaseVersion       string `json:"releaseVersion"`
		ClusterProtocolMajor uint32 `json:"clusterProtocolMajor"`
		ClusterProtocolMinor uint32 `json:"clusterProtocolMinor"`
		StateSchemaMajor     uint32 `json:"stateSchemaMajor"`
		StateSchemaMinor     uint32 `json:"stateSchemaMinor"`
	}{
		MemberID: joinerID, Address: joinerPeerAddress, DNSSAN: joinerDNS,
		CertificateSHA256: hex.EncodeToString(joinerFingerprint[:]), ReleaseVersion: buildReleaseVersion,
		ClusterProtocolMajor: 2, StateSchemaMajor: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	stageResponse, err := clusterRuntimeTestDoRequest(ctx, client, http.MethodPost,
		"http://"+leaderAddress+"/api/v1/cluster/members", stageBody)
	if err != nil {
		t.Fatalf("stage in-process joiner: %v", err)
	}
	stageOperationID := clusterRuntimeTestReadOperation(t, stageResponse)
	if stageResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("stage joiner status = %d, want 202", stageResponse.StatusCode)
	}
	tailLeader := clusterRuntimeTestLeader(t, ctx, client, managementAddresses)
	tailBackup := clusterRuntimeTestReadBackup(t, ctx, client, tailLeader)
	if tailBackup.Manifest.GetCommitIndex() <= initialBackup.Manifest.GetCommitIndex() ||
		tailBackup.Manifest.GetSnapshotIndex() != initialBackup.Manifest.GetSnapshotIndex() ||
		tailBackup.Manifest.GetEntryCount() == 0 {
		t.Fatalf("backup omitted the committed membership tail: initial=%+v current=%+v", initialBackup.Manifest, tailBackup.Manifest)
	}
	hasCommand := false
	for _, entry := range tailBackup.Entries {
		if entry.GetIndex() > tailBackup.Manifest.GetSnapshotIndex() && len(entry.GetCommand()) != 0 {
			hasCommand = true
			break
		}
	}
	if !hasCommand {
		t.Fatalf("backup contains no committed command after snapshot index %d", tailBackup.Manifest.GetSnapshotIndex())
	}
	restoredSeed, err := clusterRestoreSeed(configs[0], tailBackup)
	if err != nil {
		t.Fatalf("reconstruct staged membership from backup tail: %v", err)
	}
	if restoredSeed.SnapshotIndex != tailBackup.Manifest.GetCommitIndex() {
		t.Fatalf("restored seed index=%d, backup commit=%d", restoredSeed.SnapshotIndex, tailBackup.Manifest.GetCommitIndex())
	}

	expectedNodes++
	go func() {
		results <- nodeResult{id: joinerID, err: runClusterMode(ctx, joinerConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))}
	}()
	clusterRuntimeTestWaitOperation(t, ctx, client, append(managementAddresses, joinerManagementAddress), stageOperationID, "SUCCEEDED")
	clusterRuntimeTestWaitReady(t, client, append(managementAddresses, joinerManagementAddress))

	leaderAddress = clusterRuntimeTestLeader(t, ctx, client, append(managementAddresses, joinerManagementAddress))
	removeResponse, err := clusterRuntimeTestDoRequest(ctx, client, http.MethodDelete,
		"http://"+leaderAddress+"/api/v1/cluster/members/3", nil)
	if err != nil {
		t.Fatalf("remove in-process member: %v", err)
	}
	removeOperationID := clusterRuntimeTestReadOperation(t, removeResponse)
	if removeResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("remove member status = %d, want 202", removeResponse.StatusCode)
	}
	allAddresses := append(managementAddresses, joinerManagementAddress)
	clusterRuntimeTestWaitOperation(t, ctx, client, allAddresses, removeOperationID, "SUCCEEDED")
	clusterRuntimeTestWaitRemovalReady(t, client, allAddresses, 2)
}

func clusterRuntimeTestAssertNodeListing(t *testing.T, ctx context.Context, client *http.Client, address string) {
	t.Helper()
	type nodeListing struct {
		Data []struct {
			NodeID string `json:"nodeId"`
		} `json:"data"`
		Page struct {
			NextCursor string `json:"nextCursor"`
			HasMore    bool   `json:"hasMore"`
		} `json:"page"`
	}
	requestListing := func(query string) nodeListing {
		response, err := clusterRuntimeTestDoRequest(ctx, client, http.MethodGet,
			"http://"+address+"/api/v1/nodes"+query, nil)
		if err != nil {
			t.Fatalf("list in-process nodes: %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("list nodes status = %d, want 200", response.StatusCode)
		}
		var listing nodeListing
		if err := json.NewDecoder(response.Body).Decode(&listing); err != nil {
			t.Fatalf("decode node listing: %v", err)
		}
		return listing
	}
	first := requestListing("?limit=1")
	if len(first.Data) != 1 || !first.Page.HasMore || first.Page.NextCursor == "" {
		t.Fatalf("first node page = %+v, want one row and a continuation cursor", first)
	}
	second := requestListing("?limit=1&cursor=" + url.QueryEscape(first.Page.NextCursor))
	if len(second.Data) != 1 || second.Data[0].NodeID == first.Data[0].NodeID {
		t.Fatalf("second node page = %+v, want the next distinct node", second)
	}
	response, err := clusterRuntimeTestDoRequest(ctx, client, http.MethodGet,
		"http://"+address+"/api/v1/nodes/"+first.Data[0].NodeID, nil)
	if err != nil {
		t.Fatalf("get in-process node: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get node status = %d, want 200", response.StatusCode)
	}
}

func clusterRuntimeTestDoRequest(ctx context.Context, client *http.Client, method, url string, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer admin-token")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if method == http.MethodPost || method == http.MethodDelete {
		request.Header.Set("Idempotency-Key", "inprocess-"+strings.ToLower(method))
	}
	return client.Do(request)
}
func clusterRuntimeTestReadBackup(t *testing.T, ctx context.Context, client *http.Client, address string) *records.BackupArchive {
	t.Helper()
	response, err := clusterRuntimeTestDoRequest(ctx, client, http.MethodPost, "http://"+address+"/api/v1/cluster/backups", nil)
	if err != nil {
		t.Fatalf("download in-process cluster backup: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("backup status=%d body=%s", response.StatusCode, body)
	}
	if got := response.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("backup content type=%q", got)
	}
	encoded, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read backup archive: %v", err)
	}
	archive, err := records.DecodeBackupArchive(encoded)
	if err != nil {
		t.Fatalf("decode downloaded backup archive: %v", err)
	}
	if len(encoded) < sha256.Size {
		t.Fatalf("backup archive length=%d, smaller than its SHA-256 trailer", len(encoded))
	}
	trailer := encoded[len(encoded)-sha256.Size:]
	if got := response.Header.Get("X-NaruMQ-Backup-SHA256"); got != hex.EncodeToString(trailer) {
		t.Fatalf("backup checksum header=%q, want trailer %x", got, trailer)
	}
	if got := response.Header.Get("X-NaruMQ-Backup-Commit-Index"); got != strconv.FormatUint(archive.Manifest.GetCommitIndex(), 10) {
		t.Fatalf("backup commit header=%q, archive commit=%d", got, archive.Manifest.GetCommitIndex())
	}
	if archive.Manifest.GetBoundaryState().GetLastIncludedIndex() != archive.Manifest.GetCommitIndex() {
		t.Fatalf("backup boundary summary index=%d, commit=%d",
			archive.Manifest.GetBoundaryState().GetLastIncludedIndex(), archive.Manifest.GetCommitIndex())
	}
	return archive
}

type clusterRuntimeTestOperationEnvelope struct {
	Data struct {
		OperationID string `json:"operationId"`
		Phase       string `json:"phase"`
	} `json:"data"`
}

func clusterRuntimeTestReadOperation(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	var envelope clusterRuntimeTestOperationEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode operation response: %v", err)
	}
	if envelope.Data.OperationID == "" {
		t.Fatal("operation response has an empty operation ID")
	}
	return envelope.Data.OperationID
}

func clusterRuntimeTestLeader(t *testing.T, ctx context.Context, client *http.Client, addresses []string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, address := range addresses {
			response, err := clusterRuntimeTestDoRequest(ctx, client, http.MethodGet, "http://"+address+"/api/v1/cluster", nil)
			if err != nil {
				continue
			}
			var envelope struct {
				Data struct {
					LeaderNodeID *string `json:"leaderNodeId"`
				} `json:"data"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&envelope)
			_ = response.Body.Close()
			if decodeErr != nil || response.StatusCode != http.StatusOK || envelope.Data.LeaderNodeID == nil {
				continue
			}
			var leaderID uint64
			if _, err := fmt.Sscan(*envelope.Data.LeaderNodeID, &leaderID); err != nil || leaderID == 0 || leaderID > uint64(len(addresses)) {
				continue
			}
			return addresses[leaderID-1]
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for an in-process cluster leader")
	return ""
}

func clusterRuntimeTestWaitOperation(t *testing.T, ctx context.Context, client *http.Client, addresses []string, operationID, wantPhase string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	lastPhase := ""
	for time.Now().Before(deadline) {
		for _, address := range addresses {
			response, err := clusterRuntimeTestDoRequest(ctx, client, http.MethodGet,
				"http://"+address+"/api/v1/cluster/membership-operations/"+operationID, nil)
			if err != nil {
				continue
			}
			var envelope clusterRuntimeTestOperationEnvelope
			decodeErr := json.NewDecoder(response.Body).Decode(&envelope)
			_ = response.Body.Close()
			if decodeErr != nil || response.StatusCode != http.StatusOK {
				continue
			}
			lastPhase = envelope.Data.Phase
			if lastPhase == wantPhase {
				return
			}
			if lastPhase == "FAILED" {
				t.Fatalf("operation %s entered FAILED phase", operationID)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for operation %s: %v", operationID, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("operation %s phase = %q, want %s", operationID, lastPhase, wantPhase)
}

func clusterRuntimeTestWaitReady(t *testing.T, client *http.Client, addresses []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, address := range addresses {
			response, err := client.Get("http://" + address + "/health/ready")
			if err != nil {
				ready = false
				continue
			}
			if response.StatusCode != http.StatusOK {
				ready = false
			}
			_ = response.Body.Close()
		}
		if ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for all in-process nodes to become ready")
}

func clusterRuntimeTestWaitRemovalReady(t *testing.T, client *http.Client, addresses []string, retiredIndex int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	lastStatuses := make([]string, len(addresses))
	for time.Now().Before(deadline) {
		converged := true
		for index, address := range addresses {
			response, err := client.Get("http://" + address + "/health/ready")
			if err != nil {
				lastStatuses[index] = err.Error()
				converged = false
				continue
			}
			lastStatuses[index] = fmt.Sprintf("%d", response.StatusCode)
			wantStatus := http.StatusOK
			if index == retiredIndex {
				wantStatus = http.StatusServiceUnavailable
			}
			if response.StatusCode != wantStatus {
				converged = false
			}
			_ = response.Body.Close()
		}
		if converged {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for removed member readiness to converge: %v", lastStatuses)
}

func clusterRuntimeTestSeedAddress(t *testing.T, address string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split test peer address %q: %v", address, err)
	}
	return net.JoinHostPort("localhost", port)
}
func newClusterRuntimeTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "in-process test CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return parsed, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueClusterRuntimeTestCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, dnsName string, serial int64) ([]byte, []byte, [32]byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: dnsName},
		DNSNames: []string{dnsName, "localhost"}, NotBefore: time.Now().Add(-time.Minute),
		NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), sha256.Sum256(der)
}

func clusterRuntimeTestFreeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
