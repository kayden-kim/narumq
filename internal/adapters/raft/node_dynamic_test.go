package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	dynamicTestClusterID = "dynamic-cluster-test"
	addLearnerOperation  = "00000000-0000-4000-8000-000000000001"
	promoteOperation     = "00000000-0000-4000-8000-000000000002"
	removeOperation      = "00000000-0000-4000-8000-000000000003"
)

func TestDynamicMembershipChangesApplyAndReplayAfterRestart(t *testing.T) {
	root := t.TempDir()
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		node, err := Open(dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID))))
		if err != nil {
			t.Fatalf("open node %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	closeNodes := func() {
		for _, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node: %v", err)
			}
		}
	}
	var appliedEvents []CommittedConfigurationDTO
	defer closeNodes()

	pump := func() {
		t.Helper()
		for round := 0; round < 512; round++ {
			var messages []MessageDTO
			for _, nodeID := range []NodeID{1, 2, 3} {
				batch, err := nodes[nodeID].ProcessReady()
				if err != nil {
					t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
				}
				messages = append(messages, batch.Messages...)
				appliedEvents = append(appliedEvents, batch.ConfigurationChanges...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				target := nodes[message.To]
				if target == nil {
					continue
				}
				if err := target.Step(message); err != nil {
					t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
				}
			}
		}
		t.Fatal("Raft pump exceeded 512 rounds")
	}

	pump()
	for nodeID, node := range nodes {
		if !node.Status().Bootstrapped {
			t.Fatalf("node %d did not apply bootstrap membership: %+v", nodeID, node.Status())
		}
	}
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pump()
	if got := nodes[1].Status().Role; got != RoleLeader {
		t.Fatalf("node 1 role after campaign = %v, want leader", got)
	}
	if err := nodes[1].TransferLeadership(4); !errors.Is(err, ErrInvalidLeadershipTransfer) {
		t.Fatalf("transfer to non-voter error = %v, want ErrInvalidLeadershipTransfer", err)
	}
	if err := nodes[2].TransferLeadership(1); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower leadership transfer error = %v, want ErrNotLeader", err)
	}
	if err := nodes[1].TransferLeadership(2); err != nil {
		t.Fatalf("transfer leadership from node 1 to node 2: %v", err)
	}
	pump()
	if got := nodes[2].Status().Role; got != RoleLeader {
		t.Fatalf("node 2 role after leadership transfer = %v, want leader", got)
	}
	if err := nodes[2].TransferLeadership(1); err != nil {
		t.Fatalf("transfer leadership from node 2 to node 1: %v", err)
	}
	pump()
	if got := nodes[1].Status().Role; got != RoleLeader {
		t.Fatalf("node 1 role after leadership transfer back = %v, want leader", got)
	}
	snapshotData := []byte("typed-state-snapshot")
	if err := nodes[1].SnapshotWithApplicationData(snapshotData); err != nil {
		t.Fatalf("persist application snapshot: %v", err)
	}
	recoveredCommand := []byte("typed-state-command-after-snapshot")
	if err := nodes[1].Propose(recoveredCommand); err != nil {
		t.Fatalf("propose application command: %v", err)
	}
	pump()

	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: addLearnerOperation, MemberID: 4, Action: MembershipAddLearner,
	}); err != nil {
		t.Fatalf("propose learner: %v", err)
	}
	pump()
	assertDynamicMembers(t, nodes, []NodeID{1, 2, 3}, []NodeID{4})
	var addEvent CommittedConfigurationDTO
	for _, event := range appliedEvents {
		if event.OperationID == addLearnerOperation {
			addEvent = event
			break
		}
	}
	if addEvent.Index == 0 || addEvent.MemberID != 4 || addEvent.Action != MembershipAddLearner {
		t.Fatalf("committed add event = %+v", addEvent)
	}

	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: promoteOperation, MemberID: 4, Action: MembershipPromoteVoter,
	}); err != nil {
		t.Fatalf("propose promotion: %v", err)
	}
	pump()
	assertDynamicMembers(t, nodes, []NodeID{1, 2, 3, 4}, nil)

	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: removeOperation, MemberID: 4, Action: MembershipRemove,
	}); err != nil {
		t.Fatalf("propose removal: %v", err)
	}
	pump()
	assertDynamicMembers(t, nodes, []NodeID{1, 2, 3}, nil)

	if err := nodes[1].Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dynamicTestConfig(1, filepath.Join(root, "node-1")))
	if err != nil {
		t.Fatalf("reopen node 1: %v", err)
	}
	nodes[1] = reopened
	status := reopened.Status()
	if !status.Bootstrapped || !slices.Equal(status.VoterIDs, []NodeID{1, 2, 3}) || len(status.LearnerIDs) != 0 {
		t.Fatalf("replayed membership after restart = %+v", status)
	}
	if !bytes.Equal(reopened.ApplicationSnapshot(), snapshotData) {
		t.Fatalf("recovered typed snapshot = %q", reopened.ApplicationSnapshot())
	}
	commandRecords := reopened.AppliedCommandRecords()
	if len(commandRecords) != 1 || commandRecords[0].Index == 0 || !bytes.Equal(commandRecords[0].Data, recoveredCommand) {
		t.Fatalf("recovered indexed commands = %+v", commandRecords)
	}
	configurationRecords := reopened.AppliedConfigurationChanges()
	if len(configurationRecords) != 3 ||
		configurationRecords[0].OperationID != addLearnerOperation ||
		configurationRecords[1].OperationID != promoteOperation ||
		configurationRecords[2].OperationID != removeOperation {
		t.Fatalf("recovered committed configuration events = %+v", configurationRecords)
	}
}

func TestUnbootstrappedDynamicNodeAcknowledgesLeaderReplication(t *testing.T) {
	node, err := Open(Config{
		NodeID: 4, Directory: filepath.Join(t.TempDir(), "node-4"), DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x44},
	})
	if err != nil {
		t.Fatalf("open unbootstrapped joiner: %v", err)
	}
	defer node.Close()
	if status := node.Status(); status.Bootstrapped || len(status.VoterIDs) != 0 || len(status.LearnerIDs) != 0 {
		t.Fatalf("initial joiner membership = %+v, want unbootstrapped and empty", status)
	}

	step := func(message *pb.Message) BatchDTO {
		t.Helper()
		data, err := proto.Marshal(message)
		if err != nil {
			t.Fatalf("marshal Raft message: %v", err)
		}
		if err := node.Step(MessageDTO{From: NodeID(message.GetFrom()), To: NodeID(message.GetTo()), Data: data}); err != nil {
			t.Fatalf("step Raft message type %s: %v", message.GetType(), err)
		}
		batch, err := node.ProcessReady()
		if err != nil {
			t.Fatalf("process Raft message type %s: %v", message.GetType(), err)
		}
		return batch
	}
	assertResponse := func(batch BatchDTO, messageType pb.MessageType, index uint64) {
		t.Helper()
		for _, message := range batch.Messages {
			var decoded pb.Message
			if err := proto.Unmarshal(message.Data, &decoded); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if decoded.GetFrom() == 4 && decoded.GetTo() == 1 && decoded.GetType() == messageType &&
				decoded.GetIndex() == index {
				return
			}
		}
		t.Fatalf("response batch = %+v, want type=%s index=%d from=4 to=1",
			batch.Messages, messageType, index)
	}

	term := uint64(2)
	assertResponse(step(&pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: uint64Ptr(1), To: uint64Ptr(4),
		Term: &term,
	}), pb.MsgHeartbeatResp, 0)
	entry := &pb.Entry{
		Type: pb.EntryNormal.Enum(), Term: &term, Index: uint64Ptr(1), Data: []byte("catch-up"),
	}
	assertResponse(step(&pb.Message{
		Type: pb.MsgApp.Enum(), From: uint64Ptr(1), To: uint64Ptr(4),
		Term: &term, Entries: []*pb.Entry{entry},
	}), pb.MsgAppResp, 1)

	status := node.Status()
	if status.Bootstrapped || status.AppliedIndex != 0 || len(status.VoterIDs) != 0 || len(status.LearnerIDs) != 0 {
		t.Fatalf("joiner membership after replication = %+v, want no speculative configuration", status)
	}
}

func TestDynamicLearnerReplaysCommittedMembershipWithoutBootstrapState(t *testing.T) {
	root := t.TempDir()
	nodes := make(map[NodeID]*Node, 4)
	for _, nodeID := range []NodeID{1, 2, 3} {
		node, err := Open(dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID))))
		if err != nil {
			t.Fatalf("open voter %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	joiner, err := Open(Config{
		NodeID: 4, Directory: filepath.Join(root, "node-4"), DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x44},
	})
	if err != nil {
		t.Fatalf("open unbootstrapped learner: %v", err)
	}
	nodes[4] = joiner
	defer func() {
		for nodeID, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", nodeID, err)
			}
		}
	}()

	pump := func() {
		t.Helper()
		for round := 0; round < 512; round++ {
			var messages []MessageDTO
			for _, nodeID := range []NodeID{1, 2, 3, 4} {
				batch, err := nodes[nodeID].ProcessReady()
				if err != nil {
					t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
				}
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				if err := nodes[message.To].Step(message); err != nil {
					t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
				}
			}
		}
		t.Fatal("Raft pump exceeded 512 rounds")
	}

	pump()
	if err := nodes[1].Campaign(); err != nil {
		t.Fatalf("campaign node 1: %v", err)
	}
	pump()
	if status := nodes[1].Status(); status.Role != RoleLeader {
		t.Fatalf("node 1 role after campaign = %v, want leader", status.Role)
	}
	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: addLearnerOperation, MemberID: 4, Action: MembershipAddLearner,
	}); err != nil {
		t.Fatalf("propose learner: %v", err)
	}
	pump()

	leaderStatus := nodes[1].Status()
	if !slices.Equal(leaderStatus.VoterIDs, []NodeID{1, 2, 3}) ||
		!slices.Equal(leaderStatus.LearnerIDs, []NodeID{4}) {
		t.Fatalf("leader membership after add = voters %v learners %v",
			leaderStatus.VoterIDs, leaderStatus.LearnerIDs)
	}
	match, available := nodes[1].PeerMatchIndex(4)
	if !available || match < leaderStatus.CommitIndex {
		t.Fatalf("joiner replication match = %d available=%t, want >= commit %d",
			match, available, leaderStatus.CommitIndex)
	}
	joinerStatus := joiner.Status()
	if !joinerStatus.Bootstrapped ||
		!slices.Equal(joinerStatus.VoterIDs, []NodeID{1, 2, 3}) ||
		!slices.Equal(joinerStatus.LearnerIDs, []NodeID{4}) {
		t.Fatalf("joiner did not replay committed membership: %+v", joinerStatus)
	}
}

func TestDynamicApplicationSnapshotBeyondStaticLimitSurvivesRestart(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "node")
	node, err := Open(dynamicTestConfig(1, directory))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0xA5}, 4*1024*1024)
	if err := node.SnapshotWithApplicationData(payload); err != nil {
		t.Fatalf("persist dynamic snapshot: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dynamicTestConfig(1, directory))
	if err != nil {
		t.Fatalf("reopen dynamic snapshot: %v", err)
	}
	defer reopened.Close()
	if got := reopened.ApplicationSnapshot(); !bytes.Equal(got, payload) {
		t.Fatalf("recovered snapshot length = %d, want %d", len(got), len(payload))
	}
}

func dynamicTestConfig(nodeID NodeID, directory string) Config {
	return Config{
		NodeID: nodeID, Directory: directory, DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x42},
		BootstrapVoterIDs: []NodeID{1, 2, 3},
	}
}

func assertDynamicMembers(t *testing.T, nodes map[NodeID]*Node, voters, learners []NodeID) {
	t.Helper()
	for nodeID, node := range nodes {
		status := node.Status()
		if !slices.Equal(status.VoterIDs, voters) || !slices.Equal(status.LearnerIDs, learners) {
			t.Fatalf("node %d membership = voters %v learners %v, want voters %v learners %v", nodeID, status.VoterIDs, status.LearnerIDs, voters, learners)
		}
	}
}

func TestDynamicPersistenceBoundaryHookReportsMembershipReady(t *testing.T) {
	root := t.TempDir()
	var eventsMu sync.Mutex
	var events []PersistenceBoundaryDTO

	cfg1 := dynamicTestConfig(1, filepath.Join(root, "node-1"))
	cfg1.PersistenceBoundaryHook = func(b PersistenceBoundaryDTO) {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		events = append(events, PersistenceBoundaryDTO{
			Stage:                b.Stage,
			ConfigurationActions: slices.Clone(b.ConfigurationActions),
			InstallsSnapshot:     b.InstallsSnapshot,
		})
	}

	nodes := make(map[NodeID]*Node, 3)
	var err error
	nodes[1], err = Open(cfg1)
	if err != nil {
		t.Fatalf("open node 1: %v", err)
	}
	for _, nodeID := range []NodeID{2, 3} {
		node, err := Open(dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID))))
		if err != nil {
			t.Fatalf("open node %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Close()
		}
	}()

	pump := func() {
		t.Helper()
		for range 512 {
			var messages []MessageDTO
			for _, nodeID := range []NodeID{1, 2, 3} {
				node := nodes[nodeID]
				if node == nil {
					continue
				}
				batch, err := node.ProcessReady()
				if err != nil {
					t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
				}
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				target := nodes[message.To]
				if target == nil {
					continue
				}
				if err := target.Step(message); err != nil {
					t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
				}
			}
		}
		t.Fatal("Raft pump exceeded 512 rounds")
	}

	pump()
	if err := nodes[1].Campaign(); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	pump()
	if got := nodes[1].Status().Role; got != RoleLeader {
		t.Fatalf("node 1 role = %v, want leader", got)
	}

	eventsMu.Lock()
	events = nil
	eventsMu.Unlock()

	// Propose MembershipAddLearner for ID 4 from leader 1
	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: addLearnerOperation,
		MemberID:    4,
		Action:      MembershipAddLearner,
	}); err != nil {
		t.Fatalf("propose add learner: %v", err)
	}
	pump()

	eventsMu.Lock()
	learnerEvents := slices.Clone(events)
	events = nil
	eventsMu.Unlock()

	if len(learnerEvents) < 2 {
		t.Fatalf("expected at least 2 events for add learner, got %d: %+v", len(learnerEvents), learnerEvents)
	}
	var hasBefore, hasAfter bool
	for _, ev := range learnerEvents {
		if slices.Equal(ev.ConfigurationActions, []MembershipChangeAction{MembershipAddLearner}) && !ev.InstallsSnapshot {
			if ev.Stage == PersistenceBeforeReady {
				hasBefore = true
			} else if ev.Stage == PersistenceAfterReady {
				hasAfter = true
			}
		}
	}
	if !hasBefore || !hasAfter {
		t.Fatalf("missing before or after event for add learner: %+v", learnerEvents)
	}

	// Injected write failure on wal.bin during MembershipPromoteVoter
	walPath := filepath.Join(filepath.Join(root, "node-1"), walName)
	oldWrite := filePersistenceOps.write
	defer func() { filePersistenceOps.write = oldWrite }()
	filePersistenceOps.write = func(writer io.Writer, data []byte) (int, error) {
		file, ok := writer.(*os.File)
		if ok && filepath.Clean(file.Name()) == filepath.Clean(walPath) {
			return 0, syscall.ENOSPC
		}
		return oldWrite(writer, data)
	}

	eventsMu.Lock()
	events = nil
	eventsMu.Unlock()

	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: promoteOperation,
		MemberID:    4,
		Action:      MembershipPromoteVoter,
	}); err != nil {
		t.Fatalf("propose promote voter: %v", err)
	}
	_, processErr := nodes[1].ProcessReady()
	filePersistenceOps.write = oldWrite
	if processErr == nil {
		t.Fatal("expected ProcessReady to fail with injected write error")
	}

	eventsMu.Lock()
	failEvents := slices.Clone(events)
	eventsMu.Unlock()

	var failBefore, failAfter bool
	for _, ev := range failEvents {
		if slices.Equal(ev.ConfigurationActions, []MembershipChangeAction{MembershipPromoteVoter}) {
			if ev.Stage == PersistenceBeforeReady {
				failBefore = true
			} else if ev.Stage == PersistenceAfterReady {
				failAfter = true
			}
		}
	}
	if !failBefore {
		t.Fatalf("expected Before event for promote voter, got: %+v", failEvents)
	}
	if failAfter {
		t.Fatalf("expected NO After event after write failure, got: %+v", failEvents)
	}

	// Unbootstrapped learner receiving MsgSnap
	var snapEvents []PersistenceBoundaryDTO
	joiner, err := Open(Config{
		NodeID: 4, Directory: filepath.Join(t.TempDir(), "node-4-snap"), DynamicMembership: true,
		ClusterID: dynamicTestClusterID, CertificateSHA256: [32]byte{0x44},
		PersistenceBoundaryHook: func(b PersistenceBoundaryDTO) {
			snapEvents = append(snapEvents, PersistenceBoundaryDTO{
				Stage:                b.Stage,
				ConfigurationActions: slices.Clone(b.ConfigurationActions),
				InstallsSnapshot:     b.InstallsSnapshot,
			})
		},
	})
	if err != nil {
		t.Fatalf("open joiner: %v", err)
	}
	defer joiner.Close()

	term := uint64(2)
	snapMsg := &pb.Message{
		Type: pb.MsgSnap.Enum(),
		From: uint64Ptr(1),
		To:   uint64Ptr(4),
		Term: &term,
		Snapshot: &pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{
				Index: uint64Ptr(5),
				Term:  uint64Ptr(2),
				ConfState: &pb.ConfState{
					Voters:   []uint64{1, 2, 3},
					Learners: []uint64{4},
				},
			},
			Data: []byte("dummy-snapshot"),
		},
	}
	data, err := proto.Marshal(snapMsg)
	if err != nil {
		t.Fatal(err)
	}
	if err := joiner.Step(MessageDTO{From: 1, To: 4, Data: data}); err != nil {
		t.Fatalf("step MsgSnap: %v", err)
	}
	if _, err := joiner.ProcessReady(); err != nil {
		t.Fatalf("joiner ProcessReady with snapshot: %v", err)
	}

	if len(snapEvents) != 2 {
		t.Fatalf("expected 2 snapEvents, got %d: %+v", len(snapEvents), snapEvents)
	}
	if snapEvents[0].Stage != PersistenceBeforeReady || !snapEvents[0].InstallsSnapshot {
		t.Fatalf("expected snapEvents[0] BeforeReady with InstallsSnapshot=true, got %+v", snapEvents[0])
	}
	if snapEvents[1].Stage != PersistenceAfterReady || !snapEvents[1].InstallsSnapshot {
		t.Fatalf("expected snapEvents[1] AfterReady with InstallsSnapshot=true, got %+v", snapEvents[1])
	}
}

func TestDynamicStepAllowsOnlyAuthenticatedForwardedRequests(t *testing.T) {
	node, err := Open(dynamicTestConfig(1, filepath.Join(t.TempDir(), "node-1")))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		sender   NodeID
		origin   uint64
		kind     pb.MessageType
		rejected bool
		dropped  bool
	}{
		{name: "forwarded read index", sender: 3, origin: 2, kind: pb.MsgReadIndex},
		{name: "forwarded proposal without leader is dropped by Raft", sender: 3, origin: 2, kind: pb.MsgProp, dropped: true},
		{name: "forwarded leader transfer", sender: 3, origin: 2, kind: pb.MsgTransferLeader},
		{name: "replication cannot spoof", sender: 3, origin: 2, kind: pb.MsgApp, rejected: true},
		{name: "unknown relay cannot forward", sender: 4, origin: 2, kind: pb.MsgReadIndex, rejected: true},
		{name: "unknown requester cannot be forwarded", sender: 3, origin: 4, kind: pb.MsgReadIndex, rejected: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := proto.Marshal(&pb.Message{Type: test.kind.Enum(), From: &test.origin, To: uint64Ptr(1)})
			if err != nil {
				t.Fatal(err)
			}
			err = node.Step(MessageDTO{From: test.sender, To: 1, Data: data})
			if test.dropped {
				if err == nil || !strings.Contains(err.Error(), "proposal dropped") {
					t.Fatalf("Step forwarded %s = %v, want Raft proposal dropped", test.kind, err)
				}
			} else if test.rejected {
				if !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("Step forwarded %s = %v, want ErrInvalidMessage", test.kind, err)
				}
			} else if err != nil {
				t.Fatalf("Step forwarded %s = %v", test.kind, err)
			}
		})
	}
}
