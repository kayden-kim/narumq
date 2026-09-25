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
	"sync"
	"testing"
	"time"
)

const (
	clusterMembershipProcessClusterID = "process-acceptance-cluster"
	clusterMembershipProcessRelease   = "1.0.0"
	clusterMembershipProcessAdmin     = "process-acceptance-admin-token"
)

func TestDynamicClusterMembershipProcessAcceptance(t *testing.T) {
	repoRoot := clusterMembershipProcessRepoRoot(t)
	tempDir := t.TempDir()
	binaryPath := filepath.Join(tempDir, "narumq")
	if runtime.GOOS == "windows" {
		binaryPath += ".exe"
	}
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binaryPath, "./cmd/narumq")
	build.Dir = repoRoot
	buildOutput, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("build cmd/narumq executable: %v\n%s", err, buildOutput)
	}

	clusterCtx, clusterCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer clusterCancel()
	harness := newClusterMembershipProcessHarness(t, clusterCtx, repoRoot, binaryPath)
	t.Cleanup(harness.cleanup)

	ca := newClusterMembershipProcessCA(t)
	certificates := make([]clusterMembershipProcessCertificate, 4)
	for index := range certificates {
		certificates[index] = newClusterMembershipProcessCertificate(t, ca, fmt.Sprintf("node%d.example.test", index+1))
	}
	caPath := filepath.Join(tempDir, "certificates", "ca.pem")
	if err := os.MkdirAll(filepath.Dir(caPath), 0o700); err != nil {
		t.Fatalf("create certificate directory: %v", err)
	}
	clusterMembershipProcessWriteFile(t, caPath, ca.pem, 0o600)

	addresses := clusterMembershipProcessAddresses(t, 12)
	peerAddresses := addresses[0:4]
	managementAddresses := addresses[4:8]
	mqttAddresses := addresses[8:12]
	tokenPath := filepath.Join(tempDir, "cluster-admin.token")
	clusterMembershipProcessWriteFile(t, tokenPath, []byte(clusterMembershipProcessAdmin+"\n"), 0o600)
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
    "allowedClientIds": ["ready-connection-client"],
    "roles": ["client"]
  }],
  "acl": []
}`, jwksURL))
	clusterMembershipProcessWriteFile(t, securityPath, securityConfig, 0o600)

	members := make([]clusterMembershipProcessMember, 3)
	for index := range members {
		members[index] = clusterMembershipProcessMember{
			MemberID: uint64(index + 1), Address: peerAddresses[index],
			DNSSAN:            certificates[index].dnsName,
			CertificateSHA256: hex.EncodeToString(certificates[index].fingerprint[:]),
		}
	}
	bootstrapProcesses := make([]*clusterMembershipProcess, 3)
	bootstrapConfigs := make([]string, 3)
	for index := range bootstrapProcesses {
		dataDir := filepath.Join(tempDir, fmt.Sprintf("node-%d-data", index+1))
		if err := os.MkdirAll(dataDir, 0o700); err != nil {
			t.Fatalf("create bootstrap directory: %v", err)
		}
		config := clusterMembershipProcessConfig{
			Mode: "cluster", ClusterID: clusterMembershipProcessClusterID,
			NodeID: uint64(index + 1), Directory: dataDir,
			ListenAddress: peerAddresses[index], AdvertisedAddress: peerAddresses[index],
			LocalDNSName: certificates[index].dnsName, ReleaseVersion: clusterMembershipProcessRelease,
			ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
			Role: "bootstrap", BootstrapMembers: append([]clusterMembershipProcessMember(nil), members...),
			SeedAddresses: []string{}, PeerTLS: clusterMembershipProcessTLS{
				CertFile: certificates[index].certPath(tempDir), KeyFile: certificates[index].keyPath(tempDir), CAFile: caPath,
			},
			Management: clusterMembershipProcessManagement{ListenAddress: managementAddresses[index], ClusterAdminTokenFile: tokenPath},
			MQTT:       clusterMembershipProcessMQTT{TCPListenAddress: mqttAddresses[index], TLSListenAddress: "", SecurityConfigFile: securityPath},
		}
		clusterMembershipProcessWriteFile(t, config.PeerTLS.CertFile, certificates[index].certPEM, 0o600)
		clusterMembershipProcessWriteFile(t, config.PeerTLS.KeyFile, certificates[index].keyPEM, 0o600)
		bootstrapConfigs[index] = filepath.Join(tempDir, fmt.Sprintf("node-%d.json", index+1))
		clusterMembershipProcessWriteJSON(t, bootstrapConfigs[index], config)
	}

	// With only the first bootstrap voter running, readiness has no quorum. The
	// external MQTT CONNECT must be rejected before any additional voter starts.
	first, err := harness.start("bootstrap node 1", bootstrapConfigs[0], managementAddresses[0])
	if err != nil {
		t.Fatalf("start first bootstrap process: %v", err)
	}
	bootstrapProcesses[0] = first
	clusterMembershipProcessWaitForStatus(t, clusterCtx, first, "/health/live", http.StatusOK)
	readyStatus, readyBody, err := clusterMembershipProcessRequest(clusterCtx, http.MethodGet, first.managementURL()+"/health/ready", nil, "", "")
	if err != nil {
		t.Fatalf("query isolated voter readiness: %v", err)
	}
	if readyStatus != http.StatusServiceUnavailable {
		t.Fatalf("isolated bootstrap voter readiness status=%d body=%s, want 503", readyStatus, readyBody)
	}
	clusterMembershipProcessAssertMQTTRejected(t, mqttAddresses[0])

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

	joinerCert := certificates[3]
	joinerConfigPath := filepath.Join(tempDir, "node-4.json")
	joinerDataDir := filepath.Join(tempDir, "node-4-data")
	if err := os.MkdirAll(joinerDataDir, 0o700); err != nil {
		t.Fatalf("create joiner directory: %v", err)
	}
	joinerConfig := clusterMembershipProcessConfig{
		Mode: "cluster", ClusterID: clusterMembershipProcessClusterID, NodeID: 4,
		Directory: joinerDataDir, ListenAddress: peerAddresses[3], AdvertisedAddress: peerAddresses[3],
		LocalDNSName: joinerCert.dnsName, ReleaseVersion: clusterMembershipProcessRelease,
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
		Role: "joiner", BootstrapMembers: []clusterMembershipProcessMember{},
		SeedAddresses: []string{
			"localhost:" + clusterMembershipProcessPort(peerAddresses[0]),
			"localhost:" + clusterMembershipProcessPort(peerAddresses[1]),
			"localhost:" + clusterMembershipProcessPort(peerAddresses[2]),
		},
		PeerTLS: clusterMembershipProcessTLS{
			CertFile: joinerCert.certPath(tempDir), KeyFile: joinerCert.keyPath(tempDir), CAFile: caPath,
		},
		Management: clusterMembershipProcessManagement{ListenAddress: managementAddresses[3], ClusterAdminTokenFile: tokenPath},
		MQTT:       clusterMembershipProcessMQTT{TCPListenAddress: mqttAddresses[3], TLSListenAddress: "", SecurityConfigFile: securityPath},
	}
	clusterMembershipProcessWriteFile(t, joinerConfig.PeerTLS.CertFile, joinerCert.certPEM, 0o600)
	clusterMembershipProcessWriteFile(t, joinerConfig.PeerTLS.KeyFile, joinerCert.keyPEM, 0o600)
	clusterMembershipProcessWriteJSON(t, joinerConfigPath, joinerConfig)

	request := clusterMembershipProcessStageRequest{
		MemberID: 4, Address: peerAddresses[3], DNSSAN: joinerCert.dnsName,
		CertificateSHA256:    hex.EncodeToString(joinerCert.fingerprint[:]),
		ReleaseVersion:       clusterMembershipProcessRelease,
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
	}
	requestBody, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}
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

	retiredIdentity := request
	retiredIdentity.MemberID = removalTargetID
	retiredIdentity.Address = peerAddresses[removalTargetID-1]
	retiredIdentity.DNSSAN = certificates[removalTargetID-1].dnsName
	retiredIdentity.CertificateSHA256 = hex.EncodeToString(certificates[removalTargetID-1].fingerprint[:])
	retiredBody, err := json.Marshal(retiredIdentity)
	if err != nil {
		t.Fatalf("encode retired ID reuse request: %v", err)
	}
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

func (h *clusterMembershipProcessHarness) start(name, configPath, managementAddress string) (*clusterMembershipProcess, error) {
	processCtx, cancel := context.WithCancel(h.ctx)
	command := exec.CommandContext(processCtx, h.binary, "-cluster-config", configPath)
	command.Dir = h.root
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
	Mode                 string                             `json:"mode"`
	ClusterID            string                             `json:"clusterId"`
	NodeID               uint64                             `json:"nodeId"`
	Directory            string                             `json:"directory"`
	ListenAddress        string                             `json:"listenAddress"`
	AdvertisedAddress    string                             `json:"advertisedAddress"`
	LocalDNSName         string                             `json:"localDnsName"`
	ReleaseVersion       string                             `json:"releaseVersion"`
	ClusterProtocolMajor uint32                             `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32                             `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32                             `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32                             `json:"stateSchemaMinor"`
	Role                 string                             `json:"role"`
	BootstrapMembers     []clusterMembershipProcessMember   `json:"bootstrapMembers"`
	SeedAddresses        []string                           `json:"seedAddresses"`
	PeerTLS              clusterMembershipProcessTLS        `json:"peerTLS"`
	Management           clusterMembershipProcessManagement `json:"management"`
	MQTT                 clusterMembershipProcessMQTT       `json:"mqtt"`
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
func clusterMembershipProcessJWTFixture(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate MQTT process-test signing key: %v", err)
	}
	signingKey, err := jwk.Import(privateKey)
	if err != nil {
		t.Fatalf("import MQTT process-test signing key: %v", err)
	}
	if err := signingKey.Set("kid", "process-acceptance-key"); err != nil {
		t.Fatalf("set process-test signing key ID: %v", err)
	}
	if err := signingKey.Set("alg", "RS256"); err != nil {
		t.Fatalf("set process-test signing algorithm: %v", err)
	}
	publicKey, err := jwk.PublicKeyOf(signingKey)
	if err != nil {
		t.Fatalf("derive MQTT process-test public key: %v", err)
	}
	if err := publicKey.Set("kid", "process-acceptance-key"); err != nil {
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

	token := jwt.New()
	for name, value := range map[string]any{
		"iss": "https://issuer.example",
		"aud": []string{"narumq"},
		"sub": "mqtt-process-client",
		"exp": time.Now().Add(time.Hour),
	} {
		if err := token.Set(name, value); err != nil {
			t.Fatalf("set process-test JWT claim %q: %v", name, err)
		}
	}
	signedToken, err := jwt.Sign(token, jwt.WithKey(jwa.RS256(), signingKey))
	if err != nil {
		t.Fatalf("sign MQTT process-test JWT: %v", err)
	}
	return server.URL + "/jwks.json", string(signedToken)
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
		listeners = append(listeners, listener)
		addresses = append(addresses, listener.Addr().String())
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
			if summary.ClusterID != clusterMembershipProcessClusterID || summary.ReleaseVersion != clusterMembershipProcessRelease || summary.MemberCount != 3 || !summary.QuorumAvailable || summary.LeaderNodeID == nil || *summary.LeaderNodeID == "" {
				return false, fmt.Sprintf("%s cluster summary=%+v, want 3 voters, quorum, and leader", process.name, summary), nil
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
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
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
