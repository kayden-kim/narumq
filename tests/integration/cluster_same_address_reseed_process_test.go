package integration

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

// clusterSameAddressReseedFixture returns a view of a three-voter fixture that also describes
// member 4. Member 4 takes over slot 3: the same peer, management and MQTT addresses and the same
// DNS SAN, with a freshly issued certificate (a new key and so a different SHA-256).
func clusterSameAddressReseedFixture(t *testing.T, base *clusterMembershipProcessFixture) *clusterMembershipProcessFixture {
	t.Helper()
	const slot = 2
	reseed := *base
	reseed.certificates = append(slices.Clone(base.certificates), newClusterMembershipProcessCertificate(t, base.ca, base.certificates[slot].dnsName))
	reseed.peerAddresses = append(slices.Clone(base.peerAddresses), base.peerAddresses[slot])
	reseed.managementAddresses = append(slices.Clone(base.managementAddresses), base.managementAddresses[slot])
	reseed.mqttAddresses = append(slices.Clone(base.mqttAddresses), base.mqttAddresses[slot])
	reseed.webSocketAddresses = append(slices.Clone(base.webSocketAddresses), base.webSocketAddresses[slot])
	old, replacement := reseed.stageRequest(3), reseed.stageRequest(4)
	if old.Address != replacement.Address || old.DNSSAN != replacement.DNSSAN {
		t.Fatalf("fixture error: member 4 must reuse member 3 address and DNS SAN: old=%+v new=%+v", old, replacement)
	}
	if old.CertificateSHA256 == replacement.CertificateSHA256 {
		t.Fatalf("fixture error: member 4 must use a new certificate, both SHA-256 values are %s", old.CertificateSHA256)
	}
	return &reseed
}

// Traceability: CLU-14; ADR-0061; docs/specs/cluster-session-and-delivery.md §11.20.
//
// A lost voter's peer address and DNS SAN are reused by a new member ID with a new certificate.
// This is the cluster-side assumption behind the EKS slot replacement in ADR-0061.
func TestDynamicClusterSameAddressReseedProcessAcceptance(t *testing.T) {
	t.Parallel()
	fixture := clusterSameAddressReseedFixture(t, newClusterMembershipProcessFixture(t, 3, ""))
	ctx := fixture.ctx
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		process, err := fixture.harness.start(fmt.Sprintf("bootstrap node %d", index+1), fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, processes)
	clusterMembershipProcessPublishRetained(t, fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher", "retained/volume", []byte("v1"))
	offline := clusterMembershipProcessOpenSubscribedSession(t, fixture.mqttAddresses[2], fixture.accessToken, "offline-subscriber", "offline/#")
	_ = offline.Close()
	_, leaderURL := clusterMembershipProcessFindLeader(t, ctx, processes, 3)
	clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 1)
	clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/volume", []byte("kept"))
	clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 2)

	if err := processes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("crash voter 3: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(fixture.tempDir, "node-3-data")); err != nil {
		t.Fatalf("remove lost voter volume: %v", err)
	}
	remaining := []*clusterMembershipProcess{processes[0], processes[1]}
	for _, process := range remaining {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
	_, leaderURL = clusterMembershipProcessFindLeader(t, ctx, remaining, 3)

	// The stage request carries member 3's address and DNS SAN with member 4's certificate hash.
	stageBody := fixture.stageRequestBody(4)
	var staged clusterMembershipProcessStageRequest
	if err := json.Unmarshal(stageBody, &staged); err != nil || staged.Address != fixture.peerAddresses[2] || staged.DNSSAN != fixture.certificates[2].dnsName ||
		staged.CertificateSHA256 == hex.EncodeToString(fixture.certificates[2].fingerprint[:]) {
		t.Fatalf("stage request for member 4 = %s (decode=%v), want member 3 address and DNS SAN with a different certificate hash", stageBody, err)
	}
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost, leaderURL+"/api/v1/cluster/members", stageBody, clusterMembershipProcessAdmin, "same-address-stage-4")
	stage, decodeErr := clusterMembershipProcessDecodeOperation(body)
	if err != nil || decodeErr != nil || status != http.StatusAccepted || stage.Phase != "STAGED" {
		t.Fatalf("stage member 4 on member 3 address status=%d body=%s err=%v decode=%v", status, body, err, decodeErr)
	}
	// The new certificate replaces the slot certificate file, as the EKS Secret update does.
	joiner, err := fixture.harness.start("replacement joiner 4 on member 3 address", fixture.joinerConfig(4, []uint64{1, 2}), fixture.managementAddresses[3])
	if err != nil {
		t.Fatalf("start member 4 on member 3 address: %v", err)
	}
	clusterMembershipProcessWaitForOperation(t, ctx, processes[0], stage.OperationID, "SUCCEEDED")
	clusterMembershipProcessWaitForStatus(t, ctx, joiner, "/health/ready", http.StatusOK)
	_, leaderURL = clusterMembershipProcessFindLeader(t, ctx, remaining, 4)
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodDelete, leaderURL+"/api/v1/cluster/members/3", nil, clusterMembershipProcessAdmin, "same-address-remove-3")
	removal, decodeErr := clusterMembershipProcessDecodeOperation(body)
	if err != nil || decodeErr != nil || status != http.StatusAccepted || removal.Phase != "REMOVING" {
		t.Fatalf("remove lost voter 3 status=%d body=%s err=%v decode=%v", status, body, err, decodeErr)
	}
	clusterMembershipProcessWaitForOperation(t, ctx, processes[0], removal.OperationID, "SUCCEEDED")
	survivors := []*clusterMembershipProcess{processes[0], processes[1], joiner}
	clusterMembershipProcessWaitForClusterSize(t, ctx, survivors, 3)
	clusterMembershipProcessAssertVotersAndRetired(t, ctx, survivors, []string{"1", "2", "4"}, "3")

	// Member 4 answers on member 3's MQTT address, so these reads cover the reused slot address.
	if fixture.mqttAddresses[3] != fixture.mqttAddresses[2] {
		t.Fatalf("fixture error: member 4 MQTT address %s != member 3 %s", fixture.mqttAddresses[3], fixture.mqttAddresses[2])
	}
	retained := clusterMembershipProcessOpenCleanSubscribedSession(t, fixture.mqttAddresses[3], fixture.accessToken, "retained-subscriber-joiner", "retained/volume")
	defer retained.Close()
	if got := clusterMembershipProcessReadRetained(t, retained, 1); !reflect.DeepEqual(got, map[string]string{"retained/volume": "v1"}) {
		t.Fatalf("replacement retained=%v", got)
	}
	resumed, present := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[3], fixture.accessToken, "offline-subscriber")
	defer resumed.Close()
	if !present {
		t.Fatal("replacement lost persistent session")
	}
	if got := clusterMembershipProcessReadPublishes(t, resumed, 1); !reflect.DeepEqual(got, [][2]string{{"offline/volume", "kept"}}) {
		t.Fatalf("replacement offline messages=%v", got)
	}

	// The retired ID cannot be staged again, even with its original address and certificate.
	_, leaderURL = clusterMembershipProcessFindLeader(t, ctx, survivors, 3)
	status, body, err = clusterMembershipProcessRequest(ctx, http.MethodPost, leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(3), clusterMembershipProcessAdmin, "same-address-restage-3")
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err != nil || status != http.StatusConflict || json.Unmarshal(body, &envelope) != nil || envelope.Error.Code != "member_id_reserved" {
		t.Fatalf("re-stage retired member 3: status=%d body=%s err=%v", status, bytes.TrimSpace(body), err)
	}
	t.Log("same-address reseed: voters {1,2,4}; retired 3; retained v1 and offline kept via member 4; member 3 re-stage rejected with member_id_reserved")
}
