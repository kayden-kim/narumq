package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Traceability: CLU-02/CLU-07/CLU-14; ADR-0024; cluster spec §1, §7, §11.20.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-132, cluster-session-and-delivery.md:CLU-SCN-133, cluster-session-and-delivery.md:CLU-SCN-134
func TestDynamicClusterJoinerVoterVolumeLossProcessAcceptance(t *testing.T) {
	t.Parallel()

	for _, state := range []struct {
		name             string
		replaceBootstrap bool
	}{
		{name: "four_voters_before_replacement"},
		{name: "three_voters_after_replacement", replaceBootstrap: true},
	} {
		t.Run(state.name, func(t *testing.T) {
			fixture := newClusterMembershipProcessFixture(t, 5, "")
			ctx := fixture.ctx
			byID := make(map[uint64]*clusterMembershipProcess, 5)
			bootstrap := make([]*clusterMembershipProcess, 3)
			for index := range bootstrap {
				id := uint64(index + 1)
				process, err := fixture.harness.start(fmt.Sprintf("bootstrap %d", id), fixture.bootstrapConfig(id), fixture.managementAddresses[index])
				if err != nil {
					t.Fatalf("start bootstrap %d: %v", id, err)
				}
				bootstrap[index], byID[id] = process, process
			}
			clusterMembershipProcessWaitForBootstrap(t, ctx, bootstrap)
			stage := func(id uint64, count int) string {
				t.Helper()
				_, leaderURL := clusterJoinerVolumeLossLeader(t, ctx, byID, count)
				status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost, leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(id), clusterMembershipProcessAdmin, fmt.Sprintf("joiner-volume-stage-%d", id))
				operation, decodeErr := clusterMembershipProcessDecodeOperation(body)
				if err != nil || decodeErr != nil || status != http.StatusAccepted || operation.MemberID != id || operation.Phase != "STAGED" {
					t.Fatalf("stage %d: status=%d body=%s err=%v decode=%v", id, status, body, err, decodeErr)
				}
				return operation.OperationID
			}
			remove := func(id uint64, count int) {
				t.Helper()
				_, leaderURL := clusterJoinerVolumeLossLeader(t, ctx, byID, count)
				status, body, err := clusterMembershipProcessRequest(ctx, http.MethodDelete, leaderURL+fmt.Sprintf("/api/v1/cluster/members/%d", id), nil, clusterMembershipProcessAdmin, fmt.Sprintf("joiner-volume-remove-%d", id))
				operation, decodeErr := clusterMembershipProcessDecodeOperation(body)
				if err != nil || decodeErr != nil || status != http.StatusAccepted || operation.MemberID != id || operation.Phase != "REMOVING" {
					t.Fatalf("remove %d: status=%d body=%s err=%v decode=%v", id, status, body, err, decodeErr)
				}
				clusterMembershipProcessWaitForOperation(t, ctx, byID[1], operation.OperationID, "SUCCEEDED")
			}
			operation4 := stage(4, 3)
			config4 := fixture.joinerConfig(4, []uint64{1, 2, 3})
			joiner4, err := fixture.harness.start("promoted joiner 4", config4, fixture.managementAddresses[3])
			if err != nil {
				t.Fatalf("start joiner 4: %v", err)
			}
			byID[4] = joiner4
			clusterMembershipProcessWaitForOperation(t, ctx, byID[1], operation4, "SUCCEEDED")
			all := append(append([]*clusterMembershipProcess(nil), bootstrap...), joiner4)
			clusterMembershipProcessWaitForClusterSize(t, ctx, all, 4)
			count, voters, retired := 4, []string{"1", "2", "3", "4"}, ""
			survivors := bootstrap
			if state.replaceBootstrap {
				remove(3, 4)
				if err := byID[3].stop(5 * time.Second); err != nil {
					t.Fatalf("stop retired bootstrap 3: %v", err)
				}
				delete(byID, 3)
				count, voters, retired = 3, []string{"1", "2", "4"}, "3"
				survivors = bootstrap[:2]
				all = []*clusterMembershipProcess{byID[1], byID[2], joiner4}
				clusterMembershipProcessWaitForClusterSize(t, ctx, all, count)
			}

			// Same config, certificate, ID and volume must recover before testing loss.
			if err := joiner4.crash(5 * time.Second); err != nil {
				t.Fatalf("crash intact joiner 4: %v", err)
			}
			joiner4, err = fixture.harness.start("joiner 4 intact-volume restart", config4, fixture.managementAddresses[3])
			if err != nil {
				t.Fatalf("restart intact joiner 4: %v", err)
			}
			byID[4] = joiner4
			all = append(append([]*clusterMembershipProcess(nil), survivors...), joiner4)
			clusterMembershipProcessWaitForClusterSize(t, ctx, all, count)
			clusterMembershipProcessWaitForOperation(t, ctx, joiner4, operation4, "SUCCEEDED")
			clusterMembershipProcessAssertVotersAndRetired(t, ctx, all, voters, retired)
			clusterMembershipProcessAssertMQTTAccepted(t, fixture.mqttAddresses[3], fixture.accessToken)
			clusterMembershipProcessPublishRetained(t, fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher", "retained/joiner-volume", []byte("v1"))
			offline := clusterMembershipProcessOpenSubscribedSession(t, fixture.mqttAddresses[3], fixture.accessToken, "offline-subscriber", "offline/#")
			_ = offline.Close()
			_, leaderURL := clusterJoinerVolumeLossLeader(t, ctx, byID, count)
			clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 1)
			clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/joiner-volume", []byte("kept"))
			clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 2)

			if err := joiner4.crash(5 * time.Second); err != nil {
				t.Fatalf("crash joiner 4 before volume loss: %v", err)
			}
			delete(byID, 4)
			directory := filepath.Join(fixture.tempDir, "node-4-data")
			if err := os.RemoveAll(directory); err != nil {
				t.Fatalf("remove fixture volume 4: %v", err)
			}
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatalf("create empty fixture volume 4: %v", err)
			}
			clusterMembershipProcessWaitForClusterSize(t, ctx, survivors, count)
			leaderID, _ := clusterJoinerVolumeLossLeader(t, ctx, byID, count)
			// Probe the seed wire directly: the client guard must not mask a server bug.
			if accepted := clusterJoinerVolumeLossSeedAccepted(t, fixture, leaderID, 4); accepted {
				t.Errorf("%d-voter seed accepted same-ID empty-volume voter 4", count)
			}
			lost, err := fixture.harness.start("joiner 4 empty-volume restart", config4, fixture.managementAddresses[3])
			if err != nil {
				t.Fatalf("start lost joiner 4: %v", err)
			}
			clusterMembershipProcessWaitForStatus(t, ctx, lost, "/health/startup", http.StatusServiceUnavailable)
			// Three seconds cover multiple existing one-second seed rounds. Observe
			// health and a valid authenticated CONNECT, not an arbitrary log string.
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if err := lost.isAlive(); err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{"/health/startup", "/health/ready"} {
					status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet, lost.managementURL()+path, nil, "", "")
					if err != nil || status != http.StatusServiceUnavailable {
						t.Fatalf("lost voter %s: status=%d body=%s err=%v", path, status, body, err)
					}
				}
				clusterJoinerVolumeLossAssertMQTTRejected(t, fixture.mqttAddresses[3], fixture.accessToken)
				clusterMembershipProcessAssertVotersAndRetired(t, ctx, survivors, voters, retired)
				clusterMembershipProcessWaitForOperation(t, ctx, byID[1], operation4, "SUCCEEDED")
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(250 * time.Millisecond):
				}
			}
			files, err := filepath.Glob(filepath.Join(directory, "snapshot-*.bin*"))
			if err != nil || len(files) != 0 {
				t.Fatalf("rejected voter installed snapshot: %v err=%v", files, err)
			}
			if err := lost.stop(5 * time.Second); err != nil {
				t.Fatalf("stop rejected voter: %v", err)
			}
			t.Logf("%d-voter same-ID loss: seed rejected; startup/ready 503; no positive authenticated CONNACK; no snapshot or panic", count)

			if !state.replaceBootstrap {
				// Admission requires three incumbents; remove down 4 before adding 5.
				remove(4, 4)
				clusterMembershipProcessWaitForClusterSize(t, ctx, survivors, 3)
			}
			operation5 := stage(5, 3)
			seedIDs := []uint64{1, 2}
			if !state.replaceBootstrap {
				seedIDs = append(seedIDs, 3)
			}
			config5 := fixture.joinerConfig(5, seedIDs)
			joiner5, err := fixture.harness.start("replacement joiner 5", config5, fixture.managementAddresses[4])
			if err != nil {
				t.Fatalf("start replacement 5: %v", err)
			}
			byID[5] = joiner5
			clusterMembershipProcessWaitForOperation(t, ctx, byID[1], operation5, "SUCCEEDED")
			all = append(append([]*clusterMembershipProcess(nil), survivors...), joiner5)
			clusterMembershipProcessWaitForClusterSize(t, ctx, all, 4)
			if state.replaceBootstrap {
				remove(4, 4)
			} else {
				remove(3, 4)
				if err := byID[3].stop(5 * time.Second); err != nil {
					t.Fatalf("stop retired bootstrap 3: %v", err)
				}
				delete(byID, 3)
			}
			final := []*clusterMembershipProcess{byID[1], byID[2], joiner5}
			clusterMembershipProcessWaitForClusterSize(t, ctx, final, 3)
			clusterMembershipProcessAssertVotersAndRetired(t, ctx, final, []string{"1", "2", "5"}, "4")
			retained := clusterMembershipProcessOpenCleanSubscribedSession(t, fixture.mqttAddresses[4], fixture.accessToken, "retained-subscriber-joiner", "retained/joiner-volume")
			gotRetained := clusterMembershipProcessReadRetained(t, retained, 1)
			_ = retained.Close()
			if !reflect.DeepEqual(gotRetained, map[string]string{"retained/joiner-volume": "v1"}) {
				t.Fatalf("replacement retained=%v", gotRetained)
			}
			resumed, present := clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[4], fixture.accessToken, "offline-subscriber")
			gotOffline := clusterMembershipProcessReadPublishes(t, resumed, 1)
			_ = resumed.Close()
			if !present || !reflect.DeepEqual(gotOffline, [][2]string{{"offline/joiner-volume", "kept"}}) {
				t.Fatalf("replacement present=%t offline=%v", present, gotOffline)
			}

			// Use a new message after the first was consumed; do not expect QoS 0 replay.
			_, leaderURL = clusterJoinerVolumeLossLeader(t, ctx, byID, 3)
			clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 1)
			clusterMembershipProcessPublish(t, fixture.mqttAddresses[1], fixture.accessToken, "offline-publisher", "offline/joiner-restart", []byte("after-reseed"))
			clusterBackupRestoreProcessWaitForOfflineCount(t, ctx, leaderURL, 2)
			if err := joiner5.crash(5 * time.Second); err != nil {
				t.Fatalf("crash intact replacement 5: %v", err)
			}
			joiner5, err = fixture.harness.start("replacement 5 intact-volume restart", config5, fixture.managementAddresses[4])
			if err != nil {
				t.Fatalf("restart replacement 5: %v", err)
			}
			byID[5] = joiner5
			final[2] = joiner5
			clusterMembershipProcessWaitForClusterSize(t, ctx, final, 3)
			clusterMembershipProcessWaitForOperation(t, ctx, joiner5, operation5, "SUCCEEDED")
			clusterMembershipProcessAssertVotersAndRetired(t, ctx, final, []string{"1", "2", "5"}, "4")
			resumed, present = clusterMembershipProcessReconnectSession(t, fixture.mqttAddresses[4], fixture.accessToken, "offline-subscriber")
			gotOffline = clusterMembershipProcessReadPublishes(t, resumed, 1)
			_ = resumed.Close()
			if !present || !reflect.DeepEqual(gotOffline, [][2]string{{"offline/joiner-restart", "after-reseed"}}) {
				t.Fatalf("replacement restart present=%t offline=%v", present, gotOffline)
			}
			_, leaderURL = clusterJoinerVolumeLossLeader(t, ctx, byID, 3)
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost, leaderURL+"/api/v1/cluster/members", fixture.stageRequestBody(4), clusterMembershipProcessAdmin, "joiner-volume-reuse-retired-4")
			var envelope struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err != nil || status != http.StatusConflict || json.Unmarshal(body, &envelope) != nil || envelope.Error.Code != "member_id_reserved" {
				t.Fatalf("retired 4 stage: status=%d body=%s err=%v", status, body, err)
			}
			t.Log("new-ID reseed: voters {1,2,5}; retired 4; retained v1; SessionPresent and distinct offline messages after reseed/restart; ID reuse rejected")
		})
	}
}

func clusterJoinerVolumeLossLeader(t *testing.T, ctx context.Context, byID map[uint64]*clusterMembershipProcess, count int) (uint64, string) {
	t.Helper()
	var leaderID uint64
	var leaderURL string
	clusterMembershipProcessWait(t, ctx, "live leader after membership transition", func() (bool, string, error) {
		summary, status, body, err := clusterMembershipProcessGetSummary(ctx, byID[1].managementURL(), clusterMembershipProcessAdmin)
		if err != nil || status != http.StatusOK || summary.MemberCount != count || !summary.QuorumAvailable || summary.LeaderNodeID == nil {
			return false, fmt.Sprintf("status=%d body=%s err=%v", status, body, err), nil
		}
		id, err := strconv.ParseUint(*summary.LeaderNodeID, 10, 64)
		if err != nil || byID[id] == nil || byID[id].isAlive() != nil {
			return false, "leader has no live process", nil
		}
		leaderID, leaderURL = id, byID[id].managementURL()
		return true, "", nil
	})
	return leaderID, leaderURL
}

func clusterJoinerVolumeLossAssertMQTTRejected(t *testing.T, address, accessToken string) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		t.Fatalf("connect to rejected joiner listener: %v", err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xC0, 0, 60}
	for _, field := range []string{"offline-subscriber", "jwt", accessToken} {
		body = binary.BigEndian.AppendUint16(body, uint16(len(field)))
		body = append(body, field...)
	}
	if _, err := connection.Write(clusterMembershipProcessEncodeMQTTPacket(0x10, body)); err != nil {
		if clusterMembershipProcessIsClosed(err) {
			return
		}
		t.Fatalf("rejected joiner CONNECT write: %v", err)
	}
	header, response, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil {
		if clusterMembershipProcessIsClosed(err) {
			return
		}
		t.Fatalf("rejected joiner CONNACK read: %v", err)
	}
	if header != 0x20 || len(response) != 2 || response[1] == 0 {
		t.Fatalf("lost joiner response=(%02x,%x), want negative CONNACK or connection close", header, response)
	}
}

// Direct TLS wire probe deliberately bypasses RequestSeedAdmission's client
// protection. A valid, authenticated request must receive a server rejection.
func clusterJoinerVolumeLossSeedAccepted(t *testing.T, fixture *clusterMembershipProcessFixture, seedID, candidateID uint64) bool {
	t.Helper()
	candidate := fixture.certificates[candidateID-1]
	certificate, err := tls.X509KeyPair(candidate.certPEM, candidate.keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(fixture.caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("fixture CA is not valid PEM")
	}
	connection, err := tls.Dial("tcp", fixture.peerAddresses[seedID-1], &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots,
		ServerName: fixture.certificates[seedID-1].dnsName,
	})
	if err != nil {
		t.Fatalf("seed TLS probe: %v", err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	payload := []byte("NMQJOIN2")
	payload = binary.BigEndian.AppendUint16(payload, 2)
	payload = binary.BigEndian.AppendUint16(payload, 0)
	appendString := func(value string) {
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(value)))
		payload = append(payload, value...)
	}
	appendString(clusterMembershipProcessClusterID)
	payload = binary.BigEndian.AppendUint64(payload, candidateID)
	appendString(fixture.peerAddresses[candidateID-1])
	appendString(candidate.dnsName)
	payload = append(payload, candidate.fingerprint[:]...)
	appendString(clusterMembershipProcessRelease)
	payload = binary.BigEndian.AppendUint16(payload, 1)
	payload = binary.BigEndian.AppendUint16(payload, 0)
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	frame = append(frame, payload...)
	if _, err := connection.Write(frame); err != nil {
		t.Fatalf("seed probe write: %v", err)
	}
	var header [4]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		t.Fatalf("seed probe header: %v", err)
	}
	size := binary.BigEndian.Uint32(header[:])
	if size < 13 || size > 64<<10 {
		t.Fatalf("seed response length=%d", size)
	}
	response := make([]byte, size)
	if _, err := io.ReadFull(connection, response); err != nil {
		t.Fatalf("seed probe response: %v", err)
	}
	if string(response[:8]) != "NMQJOIN2" || binary.BigEndian.Uint16(response[8:10]) != 2 || binary.BigEndian.Uint16(response[10:12]) != 0 || response[12] > 1 {
		t.Fatalf("invalid seed response=%x", response)
	}
	return response[12] == 1
}
