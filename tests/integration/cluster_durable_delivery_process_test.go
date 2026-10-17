package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// Traceability: CLU-05/CLU-06/CLU-07/CLU-09/DEP-17; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072, cluster-session-and-delivery.md:CLU-SCN-075
func TestDynamicClusterDurableQoSTakeoverProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture, nodes := clusterDurableProcessStart(t)
	ctx := fixture.ctx
	offlineSubscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-offline-sub", false)
	clusterDurableProcessSubscribe(t, offlineSubscriber, 1, "durable-takeover/#", 2)
	clusterMembershipProcessWaitForRouteProjection(t, ctx, nodes, "durable-takeover/#")
	clusterDurableProcessWriteFatal(t, offlineSubscriber, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, offlineSubscriber, 2*time.Second)
	_ = offlineSubscriber.Close()

	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-takeover-pub", true)
	defer publisher.Close()
	clusterDurableProcessPublish(t, publisher, "durable-takeover/offline/one", 1, 10, "offline-one")
	clusterDurableProcessAck(t, publisher, 4, 10)
	clusterDurableProcessPublish(t, publisher, "durable-takeover/offline/two", 2, 20, "offline-two")
	clusterDurableProcessAck(t, publisher, 5, 20)
	clusterDurableProcessPubrel(t, publisher, 20)
	clusterDurableProcessAck(t, publisher, 7, 20)

	offlineResume := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-offline-sub", false)
	defer offlineResume.Close()
	if !clusterDurableProcessSessionPresent(t, offlineResume) {
		t.Fatal("detached QoS1/QoS2 session reconnect returned SessionPresent=false")
	}
	offlineOne := clusterDurableProcessRead(t, offlineResume)
	if offlineOne.kind != 3 || offlineOne.qos != 1 || offlineOne.topic != "durable-takeover/offline/one" || offlineOne.payload != "offline-one" {
		t.Fatalf("detached QoS1 delivery=%+v", offlineOne)
	}
	clusterDurableProcessWriteFatal(t, offlineResume, 0x40, []byte{byte(offlineOne.packetID >> 8), byte(offlineOne.packetID)})
	offlineTwo := clusterDurableProcessRead(t, offlineResume)
	if offlineTwo.kind != 3 || offlineTwo.qos != 2 || offlineTwo.topic != "durable-takeover/offline/two" || offlineTwo.payload != "offline-two" {
		t.Fatalf("detached QoS2 delivery=%+v", offlineTwo)
	}
	clusterDurableProcessWriteFatal(t, offlineResume, 0x50, []byte{byte(offlineTwo.packetID >> 8), byte(offlineTwo.packetID)})
	clusterDurableProcessAck(t, offlineResume, 6, offlineTwo.packetID)
	clusterDurableProcessWriteFatal(t, offlineResume, 0x70, []byte{byte(offlineTwo.packetID >> 8), byte(offlineTwo.packetID)})
	clusterDurableProcessWriteFatal(t, offlineResume, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, offlineResume, 2*time.Second)
	_ = offlineResume.Close()

	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-offline-sub", false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("subscriber reconnect on node 2 returned SessionPresent=false")
	}
	clusterMembershipProcessWaitForRouteProjection(t, ctx, nodes, "durable-takeover/#")
	for _, process := range nodes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
	clusterDurableProcessPublish(t, publisher, "durable-takeover/one", 1, 11, "one")
	clusterDurableProcessAck(t, publisher, 4, 11)
	first := clusterDurableProcessRead(t, subscriber)
	if first.kind != 3 || first.qos != 1 || first.packetID == 0 || first.topic != "durable-takeover/one" || first.payload != "one" {
		t.Fatalf("first durable delivery=%+v, want QoS1 PUBLISH one", first)
	}
	if err := nodes[1].crash(5 * time.Second); err != nil {
		t.Fatalf("crash subscriber owner: %v", err)
	}
	// Restore quorum before the next crash by restarting the failed voter.
	restarted, err := fixture.harness.start("durable takeover node 2 restart", fixture.bootstrapConfig(2), fixture.managementAddresses[1])
	if err != nil {
		t.Fatalf("restart subscriber owner: %v", err)
	}
	nodes[1] = restarted
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	_ = subscriber.Close()
	subscriber = clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-offline-sub", false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("takeover CONNACK SessionPresent=false")
	}
	retry := clusterDurableProcessRead(t, subscriber)
	if retry.kind != 3 || retry.qos != 1 || !retry.dup || retry.packetID != first.packetID || retry.topic != first.topic {
		t.Fatalf("QoS1 takeover retransmission=%+v, want DUP PUBLISH with packet ID %d", retry, first.packetID)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(first.packetID >> 8), byte(first.packetID)})

	// QoS2 publisher state is replicated at RECEIVE before PUBREC; reconnect after
	// a process crash and finish that transaction exactly once.
	qos2Publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-takeover-qos2-pub", false)
	clusterDurableProcessPublish(t, qos2Publisher, "durable-takeover/two", 2, 12, "two")
	clusterDurableProcessAck(t, qos2Publisher, 5, 12)
	if err := nodes[1].crash(5 * time.Second); err != nil {
		t.Fatalf("crash QoS2 publisher owner: %v", err)
	}
	restarted, err = fixture.harness.start("durable takeover node 2 restart after QoS2 receive", fixture.bootstrapConfig(2), fixture.managementAddresses[1])
	if err != nil {
		t.Fatalf("restart QoS2 publisher owner: %v", err)
	}
	nodes[1] = restarted
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	_ = qos2Publisher.Close()
	qos2Publisher = clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-takeover-qos2-pub", false)
	defer qos2Publisher.Close()
	if !clusterDurableProcessSessionPresent(t, qos2Publisher) {
		t.Fatal("QoS2 publisher reconnect SessionPresent=false")
	}
	clusterDurableProcessPubrel(t, qos2Publisher, 12)
	clusterDurableProcessAck(t, qos2Publisher, 7, 12)
	second := clusterDurableProcessRead(t, subscriber)
	if second.kind != 3 || second.qos != 2 || second.topic != "durable-takeover/two" {
		t.Fatalf("QoS2 subscriber PUBLISH=%+v", second)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x50, []byte{byte(second.packetID >> 8), byte(second.packetID)})
	clusterDurableProcessAck(t, subscriber, 6, second.packetID)
	clusterDurableProcessWriteFatal(t, subscriber, 0x70, []byte{byte(second.packetID >> 8), byte(second.packetID)})
	sentinelPublisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-takeover-sentinel", true)
	defer sentinelPublisher.Close()
	clusterDurableProcessPublish(t, sentinelPublisher, "durable-takeover/after", 1, 13, "after")
	clusterDurableProcessAck(t, sentinelPublisher, 4, 13)
	sentinel := clusterDurableProcessRead(t, subscriber)
	if sentinel.kind != 3 || sentinel.topic != "durable-takeover/after" || sentinel.payload != "after" {
		t.Fatalf("QoS2 recovery was duplicated before sentinel: %+v", sentinel)
	}
}

// Traceability: CLU-07/CLU-09; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072, cluster-session-and-delivery.md:CLU-SCN-076, cluster-session-and-delivery.md:CLU-SCN-193
func TestDynamicClusterDurableQoSAckThenKillProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture, nodes := clusterDurableProcessStart(t)
	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-ack-sub", false)
	defer subscriber.Close()
	clusterDurableProcessSubscribe(t, subscriber, 1, "durable-ack/#", 1)
	clusterMembershipProcessWaitForRouteProjection(t, fixture.ctx, nodes, "durable-ack/#")
	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-ack-pub", true)
	defer publisher.Close()
	noQuorum1 := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-no-quorum-1", true)
	defer noQuorum1.Close()
	noQuorum2 := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-no-quorum-2", true)
	noQuorum3 := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-no-quorum-3", true)
	defer noQuorum3.Close()
	defer noQuorum2.Close()
	// Later CONNECTs advance committed ownership on all three nodes. Ensure
	// each local projection has caught up before testing durable routing.
	for _, process := range nodes {
		clusterMembershipProcessWaitForStatus(t, fixture.ctx, process, "/health/ready", http.StatusOK)
	}
	clusterDurableProcessPublish(t, publisher, "durable-ack/committed", 1, 21, "committed")
	clusterDurableProcessAck(t, publisher, 4, 21)
	clusterDurableProcessPublish(t, publisher, "durable-untargeted/with-quorum", 1, 22, "untargeted")
	clusterDurableProcessAck(t, publisher, 4, 22)
	if err := nodes[0].crash(5 * time.Second); err != nil {
		t.Fatalf("crash publisher node after PUBACK: %v", err)
	}
	_ = subscriber.Close()
	subscriber = clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-ack-sub", false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("ACK-then-kill subscriber reconnect returned SessionPresent=false")
	}
	message := clusterDurableProcessRead(t, subscriber)
	if message.kind != 3 || message.topic != "durable-ack/committed" || message.payload != "committed" {
		t.Fatalf("committed delivery after publisher crash=%+v", message)
	}
	_ = publisher.Close()
	restarted, err := fixture.harness.start("durable ack publisher voter restart", fixture.bootstrapConfig(1), fixture.managementAddresses[0])
	if err != nil {
		t.Fatalf("restart publisher voter before quorum-loss phase: %v", err)
	}
	nodes[0] = restarted
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)

	// Remove quorum, then demand that neither QoS1 PUBACK nor QoS2 PUBREC succeeds.
	if err := nodes[0].crash(5 * time.Second); err != nil {
		t.Fatalf("crash voter to begin quorum-loss phase: %v", err)
	}
	if err := nodes[1].crash(5 * time.Second); err != nil {
		t.Fatalf("crash second voter: %v", err)
	}
	clusterDurableProcessPublish(t, noQuorum1, "durable-ack/no-quorum-1", 1, 31, "no-quorum")
	clusterDurableProcessAssertNoSuccess(t, noQuorum1, 4, 31)
	clusterDurableProcessPublish(t, noQuorum2, "durable-ack/no-quorum-2", 2, 32, "no-quorum")
	clusterDurableProcessAssertNoSuccess(t, noQuorum2, 5, 32)
	clusterDurableProcessPublish(t, noQuorum3, "durable-untargeted/no-quorum", 1, 33, "untargeted")
	clusterDurableProcessAssertNoSuccess(t, noQuorum3, 4, 33)
	for _, index := range []int{0, 1} {
		restarted, err := fixture.harness.start(fmt.Sprintf("durable ack voter %d restart", index+1), fixture.bootstrapConfig(uint64(index+1)), fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("restart voter %d: %v", index+1, err)
		}
		nodes[index] = restarted
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
	_ = noQuorum1.Close()
	_ = noQuorum2.Close()
	_ = noQuorum3.Close()
}

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-065, cluster-session-and-delivery.md:CLU-SCN-072
func TestDynamicClusterDurableQoSBackupRestoreProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture, nodes := clusterDurableProcessStart(t)
	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-backup-sub", false)
	defer subscriber.Close()
	clusterDurableProcessSubscribe(t, subscriber, 1, "durable-backup/#", 2)
	clusterMembershipProcessWaitForRouteProjection(t, fixture.ctx, nodes, "durable-backup/#")
	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-backup-pub", false)
	defer publisher.Close()
	clusterDurableProcessPublish(t, publisher, "durable-backup/one", 1, 41, "inflight-one")
	clusterDurableProcessAck(t, publisher, 4, 41)
	clusterDurableProcessPublish(t, publisher, "durable-backup/two", 2, 42, "inflight-two")
	clusterDurableProcessAck(t, publisher, 5, 42)
	clusterDurableProcessPubrel(t, publisher, 42)
	clusterDurableProcessAck(t, publisher, 7, 42)
	one := clusterDurableProcessRead(t, subscriber)
	if one.kind != 3 || one.qos != 1 {
		t.Fatalf("backup QoS1 inflight=%+v", one)
	}
	two := clusterDurableProcessRead(t, subscriber)
	if two.kind != 3 || two.qos != 2 {
		t.Fatalf("backup QoS2 inflight=%+v", two)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x50, []byte{byte(two.packetID >> 8), byte(two.packetID)})
	clusterDurableProcessAck(t, subscriber, 6, two.packetID)
	// Keep outbound QoS1/QoS2 state and an inbound QoS2 tombstone in the archive.
	_, leaderURL := clusterMembershipProcessFindLeader(t, fixture.ctx, nodes, 3)
	archiveBytes, archive, _, err := clusterBackupRestoreProcessCaptureBackup(t, fixture.ctx, leaderURL)
	if err != nil {
		t.Fatalf("capture durable delivery backup: %v", err)
	}
	inflight := archive.Manifest.GetBoundaryState().GetInflight()
	if inflight == nil || inflight.GetCount() < 3 || len(inflight.GetSha256()) == 0 {
		t.Fatalf("backup inflight summary=%+v, want all durable message/inbound entries and digest", inflight)
	}
	archivePath := filepath.Join(fixture.tempDir, "durable-delivery.nrbak")
	clusterMembershipProcessWriteFile(t, archivePath, archiveBytes, 0o600)
	configs := make([]string, 3)
	for i := range configs {
		original := fixture.bootstrapConfig(uint64(i + 1))
		data, err := os.ReadFile(original)
		if err != nil {
			t.Fatalf("read node config: %v", err)
		}
		var config clusterMembershipProcessConfig
		if err := json.Unmarshal(data, &config); err != nil {
			t.Fatalf("decode node config: %v", err)
		}
		config.Directory = filepath.Join(fixture.tempDir, fmt.Sprintf("durable-restored-data-%d", i+1))
		config.Management.AuditDirectory = filepath.Join(fixture.tempDir, fmt.Sprintf("durable-restored-audit-%d", i+1))
		configs[i] = filepath.Join(fixture.tempDir, fmt.Sprintf("durable-restored-%d.json", i+1))
		clusterMembershipProcessWriteJSON(t, configs[i], config)
	}
	_ = subscriber.Close()
	for _, node := range nodes {
		if err := node.stop(5 * time.Second); err != nil {
			t.Fatalf("stop source cluster: %v", err)
		}
	}
	for _, config := range configs {
		command := exec.CommandContext(fixture.ctx, fixture.binary, "-cluster-config", config, "-cluster-restore", archivePath)
		command.Dir = fixture.repoRoot
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("restore durable backup: %v\n%s", err, output)
		}
	}
	restored := make([]*clusterMembershipProcess, 3)
	for i, config := range configs {
		process, err := fixture.harness.start(fmt.Sprintf("durable restored node %d", i+1), config, fixture.managementAddresses[i])
		if err != nil {
			t.Fatalf("start restored node: %v", err)
		}
		restored[i] = process
	}
	clusterBackupRestoreProcessWaitForCluster(t, fixture.ctx, restored, 3)
	resumed := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-backup-sub", false)
	defer resumed.Close()
	if !clusterDurableProcessSessionPresent(t, resumed) {
		t.Fatal("restored session CONNACK SessionPresent=false")
	}
	resumedOne := clusterDurableProcessRead(t, resumed)
	if resumedOne.kind != 3 || resumedOne.qos != 1 || !resumedOne.dup || resumedOne.packetID != one.packetID {
		t.Fatalf("restored QoS1 inflight=%+v, original=%+v", resumedOne, one)
	}
	resumedTwo := clusterDurableProcessRead(t, resumed)
	if resumedTwo.kind != 6 || resumedTwo.packetID != two.packetID {
		t.Fatalf("restored QoS2 await-PUBCOMP packet=%+v", resumedTwo)
	}
	clusterDurableProcessWriteFatal(t, resumed, 0x40, []byte{byte(resumedOne.packetID >> 8), byte(resumedOne.packetID)})
	clusterDurableProcessWriteFatal(t, resumed, 0x70, []byte{byte(resumedTwo.packetID >> 8), byte(resumedTwo.packetID)})
	// Reconnect the publisher after restore and retry PUBREL: the durable tombstone
	// must answer success without causing a second subscriber delivery.
	_ = publisher.Close()
	pubResume := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-backup-pub", false)
	defer pubResume.Close()
	clusterDurableProcessPubrel(t, pubResume, 42)
	clusterDurableProcessAck(t, pubResume, 7, 42)
	_ = resumed.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if kind, _, _, err := clusterDurableProcessReadRaw(resumed); err == nil {
		t.Fatalf("tombstone PUBREL caused duplicate subscriber delivery: kind=%d", kind)
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("reading for duplicate delivery ended with %v, want idle timeout", err)
	}
	_ = resumed.SetReadDeadline(time.Time{})
}

// Traceability: CLU-07/CLU-09; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-077
func TestDynamicClusterDurableQoS2WillProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture, nodes := clusterDurableProcessStart(t)
	ctx := fixture.ctx

	detachedSubscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-will-sub", false)
	defer detachedSubscriber.Close()
	clusterDurableProcessSubscribe(t, detachedSubscriber, 1, "durable-will/#", 2)
	observer := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-will-observer", true)
	defer observer.Close()
	clusterDurableProcessSubscribe(t, observer, 1, "durable-will/+", 0)
	clusterDurableProcessWaitForSubscriptions(t, ctx, nodes, "acceptance",
		"durable-will-observer|durable-will/+|0",
		"durable-will-sub|durable-will/#|2")

	clusterDurableProcessWriteFatal(t, detachedSubscriber, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, detachedSubscriber, 2*time.Second)
	_ = detachedSubscriber.Close()

	willPublisherOne := clusterDurableProcessConnectWill(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-will-pub-1", true,
		&clusterDurableProcessWill{topic: "durable-will/one", payload: "will-one", qos: 2})
	defer willPublisherOne.Close()
	_ = willPublisherOne.Close()
	observedWill := clusterDurableProcessRead(t, observer)
	if observedWill.kind != 3 || observedWill.qos != 0 || observedWill.topic != "durable-will/one" || observedWill.payload != "will-one" {
		t.Fatalf("QoS 0 observer Will=%+v, want durable-will/one payload will-one", observedWill)
	}
	clusterDurableProcessWriteFatal(t, observer, 0xE0, nil)
	clusterMembershipProcessAssertConnectionClosed(t, observer, 2*time.Second)
	_ = observer.Close()

	subscriber := clusterDurableProcessConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "durable-will-sub", false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("QoS2 Will subscriber reconnect returned SessionPresent=false")
	}
	willOne := clusterDurableProcessRead(t, subscriber)
	if willOne.kind != 3 || willOne.qos != 2 || willOne.packetID == 0 || willOne.topic != "durable-will/one" || willOne.payload != "will-one" {
		t.Fatalf("detached QoS2 Will delivery=%+v", willOne)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x50, []byte{byte(willOne.packetID >> 8), byte(willOne.packetID)})
	clusterDurableProcessAck(t, subscriber, 6, willOne.packetID)
	clusterDurableProcessWriteFatal(t, subscriber, 0x70, []byte{byte(willOne.packetID >> 8), byte(willOne.packetID)})
	clusterMembershipProcessWaitForRouteProjection(t, ctx, nodes, "durable-will/#")

	willPublisherTwo := clusterDurableProcessConnectWill(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-will-pub-2", true,
		&clusterDurableProcessWill{topic: "durable-will/two", payload: "will-two", qos: 2})
	defer willPublisherTwo.Close()
	_ = willPublisherTwo.Close()
	willTwo := clusterDurableProcessRead(t, subscriber)
	if willTwo.kind != 3 || willTwo.qos != 2 || willTwo.packetID == 0 || willTwo.topic != "durable-will/two" || willTwo.payload != "will-two" {
		t.Fatalf("second QoS2 Will delivery=%+v", willTwo)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x50, []byte{byte(willTwo.packetID >> 8), byte(willTwo.packetID)})
	clusterDurableProcessAck(t, subscriber, 6, willTwo.packetID)

	if err := nodes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("crash QoS2 Will subscriber owner: %v", err)
	}
	restarted, err := fixture.harness.start("durable will node 3 restart", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart QoS2 Will subscriber owner: %v", err)
	}
	nodes[2] = restarted
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	_ = subscriber.Close()

	subscriber = clusterDurableProcessConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "durable-will-sub", false)
	defer subscriber.Close()
	if !clusterDurableProcessSessionPresent(t, subscriber) {
		t.Fatal("QoS2 Will takeover reconnect returned SessionPresent=false")
	}
	takeover := clusterDurableProcessRead(t, subscriber)
	if takeover.kind == 3 {
		t.Fatalf("QoS2 Will takeover resent PUBLISH=%+v, want PUBREL %d", takeover, willTwo.packetID)
	}
	if takeover.kind != 6 || takeover.packetID != willTwo.packetID {
		t.Fatalf("QoS2 Will takeover packet=%+v, want PUBREL %d", takeover, willTwo.packetID)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x70, []byte{byte(willTwo.packetID >> 8), byte(willTwo.packetID)})
	clusterMembershipProcessWaitForRouteProjection(t, ctx, nodes, "durable-will/#")

	sentinelPublisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "durable-will-sentinel", true)
	defer sentinelPublisher.Close()
	clusterDurableProcessPublish(t, sentinelPublisher, "durable-will/after", 1, 30, "after")
	clusterDurableProcessAck(t, sentinelPublisher, 4, 30)
	sentinel := clusterDurableProcessRead(t, subscriber)
	if sentinel.kind != 3 || sentinel.qos != 1 || sentinel.packetID == 0 || sentinel.topic != "durable-will/after" || sentinel.payload != "after" {
		t.Fatalf("QoS2 Will was duplicated before sentinel: %+v", sentinel)
	}
	clusterDurableProcessWriteFatal(t, subscriber, 0x40, []byte{byte(sentinel.packetID >> 8), byte(sentinel.packetID)})
}

type clusterDurableProcessPacket struct {
	kind     byte
	dup      bool
	qos      byte
	packetID uint16
	topic    string
	payload  string
}
type clusterDurableProcessWill struct {
	topic   string
	payload string
	qos     byte
}

func clusterDurableProcessStart(t *testing.T) (*clusterMembershipProcessFixture, []*clusterMembershipProcess) {
	t.Helper()
	fixture := newClusterMembershipProcessFixture(t, 3, "")
	nodes := make([]*clusterMembershipProcess, 3)
	for i := range nodes {
		process, err := fixture.harness.start(fmt.Sprintf("durable bootstrap node %d", i+1), fixture.bootstrapConfig(uint64(i+1)), fixture.managementAddresses[i])
		if err != nil {
			t.Fatalf("start durable node %d: %v", i+1, err)
		}
		nodes[i] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, nodes)
	return fixture, nodes
}

func clusterDurableProcessConnect(t *testing.T, address, token, clientID string, clean bool) net.Conn {
	return clusterDurableProcessConnectWill(t, address, token, clientID, clean, nil)
}

func clusterDurableProcessConnectWill(t *testing.T, address, token, clientID string, clean bool, will *clusterDurableProcessWill) net.Conn {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err != nil {
			lastErr = fmt.Errorf("dial: %w", err)
		}
		if err == nil {
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			flags := byte(0xC0)
			if clean {
				flags |= 2
			}
			if will != nil {
				flags |= 0x04 | will.qos<<3
			}
			body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, flags, 0, 60}
			fields := []string{clientID}
			if will != nil {
				fields = append(fields, will.topic, will.payload)
			}
			fields = append(fields, "jwt", token)
			for _, field := range fields {
				body = append(body, byte(len(field)>>8), byte(len(field)))
				body = append(body, field...)
			}
			if writeErr := clusterDurableProcessWrite(conn, 0x10, body); writeErr != nil {
				lastErr = fmt.Errorf("write CONNECT: %w", writeErr)
			} else {
				kind, flags, payload, readErr := clusterDurableProcessReadRaw(conn)
				if readErr != nil {
					lastErr = fmt.Errorf("read CONNACK: %w", readErr)
				} else if kind == 2 && len(payload) == 2 && payload[1] == 0 {
					_ = conn.SetDeadline(time.Time{})
					conn = clusterDurableProcessWrap(conn, payload[0]&1 != 0)
					return conn
				} else {
					lastErr = fmt.Errorf("unexpected CONNACK: kind=%d flags=%#x payload=%x", kind, flags, payload)
				}
			}
			_ = conn.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("connect MQTT client %q to %s timed out: %v", clientID, address, lastErr)
	return nil
}

func clusterDurableProcessListSubscriptions(ctx context.Context, process *clusterMembershipProcess, tenantID string) ([]string, error) {
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
		process.managementURL()+"/api/v1/subscriptions?tenantId="+url.QueryEscape(tenantID), nil, clusterMembershipProcessAdmin, "")
	if err != nil {
		return nil, fmt.Errorf("%s subscription query: %w", process.name, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("%s subscription status = %d, want %d", process.name, status, http.StatusOK)
	}
	var list struct {
		Data []struct {
			TenantID    string `json:"tenantId"`
			ClientID    string `json:"clientId"`
			TopicFilter string `json:"topicFilter"`
			QoS         int    `json:"qos"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("%s decode subscriptions: %w", process.name, err)
	}
	subscriptions := make([]string, 0, len(list.Data))
	for _, subscription := range list.Data {
		if subscription.TenantID != tenantID {
			return nil, fmt.Errorf("%s subscription tenant = %q, want %q", process.name, subscription.TenantID, tenantID)
		}
		subscriptions = append(subscriptions,
			fmt.Sprintf("%s|%s|%d", subscription.ClientID, subscription.TopicFilter, subscription.QoS))
	}
	sort.Strings(subscriptions)
	return subscriptions, nil
}

func clusterDurableProcessWaitForSubscriptions(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess, tenantID string, want ...string) {
	t.Helper()
	expected := append([]string(nil), want...)
	sort.Strings(expected)
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("subscription projection %v", expected), func() (bool, string, error) {
		for _, process := range processes {
			got, err := clusterDurableProcessListSubscriptions(ctx, process, tenantID)
			if err != nil {
				return false, err.Error(), nil
			}
			if len(got) != len(expected) {
				return false, fmt.Sprintf("%s subscriptions = %v, want %v", process.name, got, expected), nil
			}
			for index := range got {
				if got[index] != expected[index] {
					return false, fmt.Sprintf("%s subscriptions = %v, want %v", process.name, got, expected), nil
				}
			}
		}
		return true, "all nodes agree on subscription projection", nil
	})
}

type clusterDurableProcessConn struct {
	net.Conn
	present bool
}

func clusterDurableProcessWrap(conn net.Conn, present bool) net.Conn {
	return clusterDurableProcessConn{Conn: conn, present: present}
}
func clusterDurableProcessSessionPresent(t *testing.T, conn net.Conn) bool {
	t.Helper()
	wrapped, ok := conn.(clusterDurableProcessConn)
	if !ok {
		t.Fatal("missing CONNECT session-present result")
	}
	return wrapped.present
}
func clusterDurableProcessSubscribe(t *testing.T, conn net.Conn, id uint16, filter string, qos byte) {
	t.Helper()
	body := []byte{byte(id >> 8), byte(id), byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	body = append(body, qos)
	if err := clusterDurableProcessWrite(conn, 0x82, body); err != nil {
		t.Fatal(err)
	}
	kind, _, payload, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 9 || len(payload) < 3 || binary.BigEndian.Uint16(payload[:2]) != id || payload[2] != qos {
		t.Fatalf("SUBACK kind=%d payload=%x err=%v", kind, payload, err)
	}
}
func clusterDurableProcessUnsubscribe(t *testing.T, conn net.Conn, id uint16, filter string) {
	t.Helper()
	body := []byte{byte(id >> 8), byte(id), byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	if err := clusterDurableProcessWrite(conn, 0xA2, body); err != nil {
		t.Fatal(err)
	}
	kind, _, payload, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 11 || len(payload) < 2 || binary.BigEndian.Uint16(payload[:2]) != id {
		t.Fatalf("UNSUBACK kind=%d payload=%x err=%v, want kind=11 packet ID=%d", kind, payload, err, id)
	}
}

func clusterDurableProcessPublish(t *testing.T, conn net.Conn, topic string, qos byte, id uint16, payload string) {
	t.Helper()
	body := []byte{byte(len(topic) >> 8), byte(len(topic))}
	body = append(body, topic...)
	if qos > 0 {
		body = append(body, byte(id>>8), byte(id))
	}
	body = append(body, payload...)
	flags := qos << 1
	if err := clusterDurableProcessWrite(conn, 0x30|flags, body); err != nil {
		t.Fatal(err)
	}
}
func clusterDurableProcessPubrel(t *testing.T, conn net.Conn, id uint16) {
	t.Helper()
	clusterDurableProcessWriteFatal(t, conn, 0x62, []byte{byte(id >> 8), byte(id)})
}
func clusterDurableProcessWriteFatal(t *testing.T, conn net.Conn, kind byte, body []byte) {
	t.Helper()
	if err := clusterDurableProcessWrite(conn, kind, body); err != nil {
		t.Fatal(err)
	}
}
func clusterDurableProcessAck(t *testing.T, conn net.Conn, want byte, id uint16) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	kind, _, body, err := clusterDurableProcessReadRaw(conn)
	if err != nil {
		t.Fatalf("read MQTT ack kind=%d packet_id=%d: %v", want, id, err)
	}
	if kind != want || len(body) < 2 || binary.BigEndian.Uint16(body[:2]) != id {
		t.Fatalf("MQTT ack kind=%d body=%x, want kind=%d packet ID=%d", kind, body, want, id)
	}
}
func clusterDurableProcessRead(t *testing.T, conn net.Conn) clusterDurableProcessPacket {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	kind, flags, body, err := clusterDurableProcessReadRaw(conn)
	if err != nil {
		t.Fatalf("read MQTT delivery: %v", err)
	}
	packet := clusterDurableProcessPacket{kind: kind, dup: flags&8 != 0, qos: (flags >> 1) & 3}
	if kind == 3 {
		if len(body) < 2 {
			t.Fatalf("short PUBLISH %x", body)
		}
		n := int(binary.BigEndian.Uint16(body))
		if len(body) < 2+n {
			t.Fatalf("short PUBLISH topic %x", body)
		}
		packet.topic = string(body[2 : 2+n])
		offset := 2 + n
		if packet.qos > 0 {
			if len(body) < offset+2 {
				t.Fatalf("short PUBLISH packet ID %x", body)
			}
			packet.packetID = binary.BigEndian.Uint16(body[offset:])
			offset += 2
		}
		packet.payload = string(body[offset:])
	} else if len(body) >= 2 {
		packet.packetID = binary.BigEndian.Uint16(body[:2])
	}
	return packet
}
func clusterDurableProcessAssertNoSuccess(t *testing.T, conn net.Conn, kind byte, id uint16) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, _, body, err := clusterDurableProcessReadRaw(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return
		}
		if clusterMembershipProcessIsClosed(err) {
			return
		}
		t.Fatalf("read non-quorum response: %v", err)
	}
	if got == kind && len(body) >= 2 && binary.BigEndian.Uint16(body[:2]) == id && (len(body) < 3 || body[2] == 0) {
		t.Fatalf("received success packet type %d for packet ID %d without quorum", kind, id)
	}
}
func clusterDurableProcessWrite(conn net.Conn, kind byte, body []byte) error {
	packet := []byte{kind}
	n := len(body)
	for {
		digit := byte(n % 128)
		n /= 128
		if n > 0 {
			digit |= 0x80
		}
		packet = append(packet, digit)
		if n == 0 {
			break
		}
	}
	packet = append(packet, body...)
	_, err := conn.Write(packet)
	return err
}
func clusterDurableProcessReadRaw(conn net.Conn) (byte, byte, []byte, error) {
	var fixed [1]byte
	if _, err := io.ReadFull(conn, fixed[:]); err != nil {
		return 0, 0, nil, err
	}
	remaining, multiplier := 0, 1
	for range 4 {
		var digit [1]byte
		if _, err := io.ReadFull(conn, digit[:]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, 0, nil, err
		}
		remaining += int(digit[0]&127) * multiplier
		if digit[0]&128 == 0 {
			body := make([]byte, remaining)
			_, err := io.ReadFull(conn, body)
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return fixed[0] >> 4, fixed[0] & 15, body, err
		}
		multiplier *= 128
	}
	return 0, 0, nil, fmt.Errorf("invalid MQTT remaining length")
}
