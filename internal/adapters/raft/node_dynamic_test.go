package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	dynamicTestClusterID = "dynamic-cluster-test"
	addLearnerOperation  = "00000000-0000-4000-8000-000000000001"
	promoteOperation     = "00000000-0000-4000-8000-000000000002"
	removeOperation      = "00000000-0000-4000-8000-000000000003"
)

// TestDynamicMembershipChangesApplyAndReplayAfterRestart verifies membership changes apply, replay after restart, and reject a pending change.
// Scenarios: performance-and-verification.md:PRF-SCN-088
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
				batch, err := processReadyForTest(t, nodes[nodeID])
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
				if _, err := target.Step(message); err != nil {
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
	if match, available := nodes[2].PeerMatchIndex(1); available || match != 0 {
		t.Fatalf("node 3 match index for node 1 = %d available=%t, want 0 and unavailable", match, available)
	}
	if match, available := nodes[1].PeerMatchIndex(2); !available || match <= 0 {
		t.Fatalf("node 2 match index from node 1 = %d available=%t, want positive and available", match, available)
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
	if err := nodes[1].ProposeMembershipChange(MembershipChangeDTO{
		OperationID: promoteOperation, MemberID: 4, Action: MembershipPromoteVoter,
	}); !errors.Is(err, ErrMembershipChangeInProgress) {
		t.Fatalf("propose promotion while learner change is pending = %v, want %v", err, ErrMembershipChangeInProgress)
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
		if _, err := node.Step(MessageDTO{From: NodeID(message.GetFrom()), To: NodeID(message.GetTo()), Data: data}); err != nil {
			t.Fatalf("step Raft message type %s: %v", message.GetType(), err)
		}
		batch, err := processReadyForTest(t, node)
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
				batch, err := processReadyForTest(t, nodes[nodeID])
				if err != nil {
					t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
				}
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				if _, err := nodes[message.To].Step(message); err != nil {
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
	if _, err := processReadyForTest(t, node); err != nil {
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
				batch, err := processReadyForTest(t, node)
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
				if _, err := target.Step(message); err != nil {
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
	_, processErr := processReadyForTest(t, nodes[1])
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
	if _, err := joiner.Step(MessageDTO{From: 1, To: 4, Data: data}); err != nil {
		t.Fatalf("step MsgSnap: %v", err)
	}
	if _, err := processReadyForTest(t, joiner); err != nil {
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

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-022
func TestDynamicStepAllowsOnlyAuthenticatedForwardedRequests(t *testing.T) {
	node, err := Open(dynamicTestConfig(1, filepath.Join(t.TempDir(), "node-1")))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if _, err := processReadyForTest(t, node); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		sender   NodeID
		origin   uint64
		kind     pb.MessageType
		rejected bool
	}{
		{name: "forwarded read index", sender: 3, origin: 2, kind: pb.MsgReadIndex},
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
			_, err = node.Step(MessageDTO{From: test.sender, To: 1, Data: data})
			if test.rejected {
				if !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("Step forwarded %s = %v, want ErrInvalidMessage", test.kind, err)
				}
			} else if err != nil {
				t.Fatalf("Step forwarded %s = %v", test.kind, err)
			}
		})
	}
}

// TestDynamicFollowerForwardsApplicationProposal verifies a dynamic follower forwards an application proposal to the leader.
// Scenarios: performance-and-verification.md:PRF-SCN-009
func TestDynamicFollowerForwardsApplicationProposal(t *testing.T) {
	root := t.TempDir()
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		node, err := Open(dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID))))
		if err != nil {
			t.Fatalf("open node %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	defer func() {
		for _, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node: %v", err)
			}
		}
	}()

	pump := func() {
		t.Helper()
		for round := 0; round < 512; round++ {
			var messages []MessageDTO
			for _, nodeID := range []NodeID{1, 2, 3} {
				batch, err := processReadyForTest(t, nodes[nodeID])
				if err != nil {
					t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
				}
				messages = append(messages, batch.Messages...)
			}
			if len(messages) == 0 {
				return
			}
			for _, message := range messages {
				if _, err := nodes[message.To].Step(message); err != nil {
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
	if nodes[1].Status().Role != RoleLeader {
		t.Fatalf("node 1 role = %v, want leader", nodes[1].Status().Role)
	}

	command := []byte("follower-application-proposal")
	if err := nodes[2].Propose(command); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose() on follower = %v, want leader-only ErrNotLeader", err)
	}
	if err := nodes[2].ProposeForwarded(command); err != nil {
		t.Fatalf("forward proposal from node 2: %v", err)
	}
	pump()
	for _, nodeID := range []NodeID{1, 2, 3} {
		commands := nodes[nodeID].AppliedCommands()
		if len(commands) != 1 || string(commands[0]) != string(command) {
			t.Fatalf("node %d applied commands = %q, want [%q]", nodeID, commands, command)
		}
	}

	batch := [][]byte{[]byte("batch-a"), []byte("batch-b"), []byte("batch-c")}
	if err := nodes[2].ProposeBatchForwarded([][]byte{[]byte("must-not-apply"), nil}); !errors.Is(err, ErrEmptyCommand) {
		t.Fatalf("invalid batch error = %v, want ErrEmptyCommand", err)
	}
	if err := nodes[2].ProposeBatchForwarded([][]byte{make([]byte, maxDynamicCommandSize/2), make([]byte, maxDynamicCommandSize/2)}); !errors.Is(err, ErrProposalTooLarge) {
		t.Fatalf("oversized forwarded frame error = %v, want ErrProposalTooLarge", err)
	}
	if err := nodes[2].ProposeBatchForwarded(batch); err != nil {
		t.Fatal(err)
	}
	// The caller can reuse its bytes before either replication or WAL sync.
	batch[0][0] = 'X'
	pump()
	want := []string{string(command), "batch-a", "batch-b", "batch-c"}
	for _, nodeID := range []NodeID{1, 2, 3} {
		got := nodes[nodeID].AppliedCommands()
		if len(got) != len(want) {
			t.Fatalf("node %d applied %q, want %q", nodeID, got, want)
		}
		for index, expected := range want {
			if string(got[index]) != expected {
				t.Fatalf("node %d command %d = %q, want %q", nodeID, index, got[index], expected)
			}
		}
		if err := nodes[nodeID].Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID))))
		if err != nil {
			t.Fatal(err)
		}
		nodes[nodeID] = reopened
		records := reopened.AppliedCommandRecords()
		if len(records) != len(want) {
			t.Fatalf("node %d replayed records = %v, want %q", nodeID, records, want)
		}
		for index, record := range records {
			if string(record.Data) != want[index] || (index > 0 && record.Index != records[index-1].Index+1) {
				t.Fatalf("node %d replayed record %d = %+v", nodeID, index, record)
			}
		}
	}
}

func openDynamicTestNodes(t *testing.T) map[NodeID]*Node {
	t.Helper()
	return openDynamicTestNodesWithCapacity(t, nil)
}

func openDynamicTestNodesWithCapacity(t *testing.T, capacity func(NodeID) (int, int)) map[NodeID]*Node {
	t.Helper()
	root := t.TempDir()
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		config := dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID)))
		config.ForwardedProposalCapacity = capacity
		node, err := Open(config)
		if err != nil {
			t.Fatalf("open node %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node: %v", err)
			}
		}
	})
	return nodes
}

func pumpDynamicNodesUntil(t *testing.T, nodes map[NodeID]*Node, done func() bool) {
	t.Helper()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-timeout.C:
			t.Fatal("Raft pump timed out")
		default:
		}
		var messages []MessageDTO
		progress := false
		for _, nodeID := range []NodeID{1, 2, 3} {
			batch, err := nodes[nodeID].ProcessReady()
			if err != nil {
				t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
			}
			progress = progress || batch.Progress
			messages = append(messages, batch.Messages...)
			if err := nodes[nodeID].CompleteApply(batch.ApplySequence); err != nil {
				t.Fatalf("node %d CompleteApply(): %v", nodeID, err)
			}
		}
		for _, message := range messages {
			if _, err := nodes[message.To].Step(message); err != nil {
				t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
			}
		}
		if done() {
			return
		}
		if len(messages) != 0 || progress {
			continue
		}
		select {
		case <-nodes[1].StorageReady():
		case <-nodes[2].StorageReady():
		case <-nodes[3].StorageReady():
		case <-timeout.C:
			t.Fatal("Raft pump timed out waiting for storage or protocol progress")
		}
	}
}

func dynamicAppliedCommandsMatch(nodes map[NodeID]*Node, want [][]byte) bool {
	for _, nodeID := range []NodeID{1, 2, 3} {
		got := nodes[nodeID].AppliedCommands()
		if len(got) != len(want) {
			return false
		}
		for index := range want {
			if !bytes.Equal(got[index], want[index]) {
				return false
			}
		}
	}
	return true
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-060
func TestDynamicRejectedProposalsReplayAfterLeadershipTransfer(t *testing.T) {
	nodes := openDynamicTestNodes(t)
	pumpDynamicNodesUntil(t, nodes, func() bool {
		for _, nodeID := range []NodeID{1, 2, 3} {
			if !nodes[nodeID].Status().Bootstrapped || nodes[nodeID].StoragePending() {
				return false
			}
		}
		return true
	})

	unknownLeaderCommand := []byte("forwarded-before-leader-known")
	unknownLeaderFrame := mustMarshal(t, &pb.Message{
		Type: pb.MsgProp.Enum(), From: uint64Ptr(3), To: uint64Ptr(1),
		Entries: []*pb.Entry{{Data: unknownLeaderCommand}},
	})
	if _, err := nodes[1].Step(MessageDTO{From: 3, To: 1, Data: unknownLeaderFrame}); err != nil {
		t.Fatalf("stage forwarded proposal without a known leader: %v", err)
	}
	want := [][]byte{unknownLeaderCommand}
	if !nodes[1].StoragePending() {
		t.Fatal("queued proposal without a known leader is not reported as pending")
	}
	if err := nodes[1].Campaign(); err != nil {
		t.Fatalf("campaign node 1: %v", err)
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		if nodes[1].Status().Role != RoleLeader || !dynamicAppliedCommandsMatch(nodes, want) {
			return false
		}
		for _, nodeID := range []NodeID{1, 2, 3} {
			if nodes[nodeID].StoragePending() {
				return false
			}
		}
		return true
	})

	acceptedCommand := []byte("accepted-before-transfer")
	if err := nodes[1].ProposeForwarded(acceptedCommand); err != nil {
		t.Fatalf("accept proposal before leadership transfer: %v", err)
	}
	if err := nodes[1].TransferLeadership(2); err != nil {
		t.Fatalf("transfer leadership from node 1 to node 2: %v", err)
	}
	localCommand := []byte("forwarded-local")
	if err := nodes[1].ProposeForwarded(localCommand); err != nil {
		t.Fatalf("stage local forwarded proposal during transfer: %v", err)
	}
	batchCommands := [][]byte{[]byte("batch-first"), []byte("batch-second")}
	if err := nodes[1].ProposeBatchForwarded(batchCommands); err != nil {
		t.Fatalf("stage forwarded batch during transfer: %v", err)
	}
	inboundCommand := []byte("inbound-forwarded")
	inboundFrame := mustMarshal(t, &pb.Message{
		Type: pb.MsgProp.Enum(), From: uint64Ptr(3), To: uint64Ptr(1),
		Entries: []*pb.Entry{{Data: inboundCommand}},
	})
	if _, err := nodes[1].Step(MessageDTO{From: 3, To: 1, Data: inboundFrame}); err != nil {
		t.Fatalf("stage authenticated inbound proposal during transfer: %v", err)
	}
	if !nodes[1].StoragePending() {
		t.Fatal("queued proposals during leadership transfer are not reported as pending")
	}
	invalidIdentity := mustMarshal(t, &pb.Message{
		Type: pb.MsgProp.Enum(), From: uint64Ptr(3), To: uint64Ptr(1),
		Entries: []*pb.Entry{{Data: []byte("invalid-identity")}},
	})
	if _, err := nodes[1].Step(MessageDTO{From: 4, To: 1, Data: invalidIdentity}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("proposal from invalid identity = %v, want ErrInvalidMessage", err)
	}
	if _, err := nodes[1].Step(MessageDTO{From: 3, To: 1, Data: []byte{0xff}}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("malformed inbound frame = %v, want ErrInvalidMessage", err)
	}

	want = [][]byte{
		unknownLeaderCommand, acceptedCommand, []byte("forwarded-local"),
		[]byte("batch-first"), []byte("batch-second"), inboundCommand,
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		if nodes[2].Status().Role != RoleLeader || nodes[1].Status().Role != RoleFollower || !dynamicAppliedCommandsMatch(nodes, want) {
			return false
		}
		for _, nodeID := range []NodeID{1, 2, 3} {
			if nodes[nodeID].StoragePending() {
				return false
			}
		}
		return true
	})
	if err := nodes[1].Propose([]byte("leader-only-on-follower")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("leader-only proposal on follower = %v, want ErrNotLeader", err)
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-061
func TestDynamicRejectedProposalStagingHasItemAndByteBounds(t *testing.T) {
	nodes := openDynamicTestNodes(t)
	pumpDynamicNodesUntil(t, nodes, func() bool {
		for _, nodeID := range []NodeID{1, 2, 3} {
			if !nodes[nodeID].Status().Bootstrapped || nodes[nodeID].StoragePending() {
				return false
			}
		}
		return true
	})
	if err := nodes[1].Campaign(); err != nil {
		t.Fatalf("campaign node 1: %v", err)
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		if nodes[1].Status().Role != RoleLeader {
			return false
		}
		for _, nodeID := range []NodeID{1, 2, 3} {
			if nodes[nodeID].StoragePending() {
				return false
			}
		}
		return true
	})

	if err := nodes[1].TransferLeadership(2); err != nil {
		t.Fatalf("transfer leadership from node 1 to node 2: %v", err)
	}
	var want [][]byte
	for index := range 64 {
		command := []byte(fmt.Sprintf("queued-%02d", index))
		if err := nodes[1].ProposeForwarded(command); err != nil {
			t.Fatalf("stage proposal %d within item limit: %v", index, err)
		}
		want = append(want, command)
	}
	if err := nodes[1].ProposeForwarded([]byte("over-item-limit")); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("proposal beyond 64-message limit = %v, want ErrBackpressure", err)
	}
	if !nodes[1].StoragePending() {
		t.Fatal("item-bounded queued proposals are not reported as pending")
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		if nodes[2].Status().Role != RoleLeader || nodes[1].Status().Role != RoleFollower || !dynamicAppliedCommandsMatch(nodes, want) {
			return false
		}
		for _, nodeID := range []NodeID{1, 2, 3} {
			if nodes[nodeID].StoragePending() {
				return false
			}
		}
		return true
	})

	if err := nodes[2].TransferLeadership(1); err != nil {
		t.Fatalf("prepare byte-bound scenario on node 1: %v", err)
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return nodes[1].Status().Role == RoleLeader && !nodes[1].StoragePending() &&
			!nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	if err := nodes[1].TransferLeadership(2); err != nil {
		t.Fatalf("transfer leadership from node 1 to node 2 for byte-bound check: %v", err)
	}
	largeCommand := bytes.Repeat([]byte{'x'}, maxDynamicCommandSize-512)
	if err := nodes[1].ProposeForwarded(largeCommand); err != nil {
		t.Fatalf("stage proposal within byte limit: %v", err)
	}
	if err := nodes[1].ProposeForwarded(largeCommand); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("proposal beyond byte limit = %v, want ErrBackpressure", err)
	}
	if !nodes[1].StoragePending() {
		t.Fatal("byte-bounded queued proposal is not reported as pending")
	}
	want = append(want, largeCommand)
	pumpDynamicNodesUntil(t, nodes, func() bool {
		if nodes[2].Status().Role != RoleLeader || nodes[1].Status().Role != RoleFollower || !dynamicAppliedCommandsMatch(nodes, want) {
			return false
		}
		for _, nodeID := range []NodeID{1, 2, 3} {
			if nodes[nodeID].StoragePending() {
				return false
			}
		}
		return true
	})
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-061
func TestDynamicDeferredProposalsRespectDownstreamCredits(t *testing.T) {
	messages, availableBytes := 0, 0
	nodes := openDynamicTestNodesWithCapacity(t, func(NodeID) (int, int) {
		return messages, availableBytes
	})
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return nodes[1].Status().Bootstrapped && nodes[2].Status().Bootstrapped &&
			nodes[3].Status().Bootstrapped && !nodes[1].StoragePending() &&
			!nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return nodes[1].Status().Role == RoleLeader && !nodes[1].StoragePending() &&
			!nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	if err := nodes[1].TransferLeadership(2); err != nil {
		t.Fatal(err)
	}
	first, second := []byte("credit-first"), bytes.Repeat([]byte{'y'}, 4096)
	for _, command := range [][]byte{first, second} {
		if err := nodes[1].ProposeForwarded(command); err != nil {
			t.Fatal(err)
		}
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return nodes[2].Status().Role == RoleLeader && nodes[1].Status().Role == RoleFollower &&
			nodes[3].Status().LeaderID == 2 && !nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	if !dynamicAppliedCommandsMatch(nodes, nil) || !nodes[1].StoragePending() {
		t.Fatal("zero downstream credits did not preserve the uncommitted FIFO")
	}
	messages, availableBytes = 1, 128
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return dynamicAppliedCommandsMatch(nodes, [][]byte{first}) &&
			!nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	if !nodes[1].StoragePending() {
		t.Fatal("byte-blocked second proposal disappeared after the first committed")
	}
	messages, availableBytes = 0, maxMessageSize
	batch, err := nodes[1].ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	if err := nodes[1].CompleteApply(batch.ApplySequence); err != nil {
		t.Fatal(err)
	}
	for _, frame := range batch.Messages {
		var decoded pb.Message
		if err := proto.Unmarshal(frame.Data, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.GetType() == pb.MsgProp {
			t.Fatal("zero message credits forwarded a queued proposal")
		}
		if _, err := nodes[frame.To].Step(frame); err != nil {
			t.Fatal(err)
		}
	}
	messages = 1
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return nodes[2].Status().Role == RoleLeader && nodes[1].Status().Role == RoleFollower &&
			dynamicAppliedCommandsMatch(nodes, [][]byte{first, second}) &&
			!nodes[1].StoragePending() && !nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
}

// Traceability: CLU-12; docs/specs/cluster-session-and-delivery.md §4.2, §11.28.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-202
func TestDynamicDeferForwardedProposalReplaysUnsentFrameOnce(t *testing.T) {
	messages, availableBytes := 0, 0
	nodes := openDynamicTestNodesWithCapacity(t, func(NodeID) (int, int) {
		return messages, availableBytes
	})
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return nodes[1].Status().Bootstrapped && nodes[2].Status().Bootstrapped &&
			nodes[3].Status().Bootstrapped && !nodes[1].StoragePending() &&
			!nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return nodes[1].Status().Role == RoleLeader && nodes[2].Status().LeaderID == 1 &&
			!nodes[1].StoragePending() && !nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	cmd := []byte("reclaimed-once")
	if err := nodes[2].ProposeForwarded(cmd); err != nil {
		t.Fatal(err)
	}
	batch, err := nodes[2].ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	var saved MessageDTO
	for _, frame := range batch.Messages {
		if frame.Type == "MsgProp" {
			if saved.Data != nil {
				t.Fatal("one proposal emitted multiple frames")
			}
			saved = frame
			continue
		}
		if _, err := nodes[frame.To].Step(frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := nodes[2].CompleteApply(batch.ApplySequence); err != nil {
		t.Fatal(err)
	}
	if saved.Data == nil {
		t.Fatal("follower did not emit a proposal frame")
	}
	before, _ := nodes[2].DeferredProposalRoom()
	if err := nodes[2].DeferForwardedProposal(saved.Data); err != nil {
		t.Fatal(err)
	}
	after, _ := nodes[2].DeferredProposalRoom()
	if after != before-1 || !nodes[2].StoragePending() {
		t.Fatalf("retained FIFO room = %d -> %d, pending=%v", before, after, nodes[2].StoragePending())
	}
	if !dynamicAppliedCommandsMatch(nodes, nil) {
		t.Fatal("unwritten proposal applied before replay")
	}
	messages, availableBytes = 1, maxMessageSize
	pumpDynamicNodesUntil(t, nodes, func() bool {
		return dynamicAppliedCommandsMatch(nodes, [][]byte{cmd}) && !nodes[1].StoragePending() &&
			!nodes[2].StoragePending() && !nodes[3].StoragePending()
	})
	heartbeat, err := proto.Marshal(&pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: proto.Uint64(1), To: proto.Uint64(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nodes[2].DeferForwardedProposal(heartbeat); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("heartbeat retained as proposal: %v", err)
	}
}
