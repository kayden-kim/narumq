package raft

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestValidateDynamicConfChangeRejectsMalformedChanges(t *testing.T) {
	validSingle := confChangeSingle(4, pb.ConfChangeAddLearnerNode)
	jointTransition := pb.ConfChangeTransition(1)
	unknownType := pb.ConfChangeType(99)
	cases := []struct {
		name   string
		change *pb.ConfChangeV2
	}{
		{name: "nil change"},
		{name: "empty context", change: &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{validSingle}}},
		{name: "noncanonical operation ID", change: dynamicConfChange("00000000-0000-4000-8000-00000000000A", validSingle)},
		{name: "multiple changes", change: dynamicConfChange(addLearnerOperation, validSingle, confChangeSingle(5, pb.ConfChangeAddLearnerNode))},
		{name: "joint transition", change: &pb.ConfChangeV2{Transition: &jointTransition, Context: []byte(addLearnerOperation), Changes: []*pb.ConfChangeSingle{validSingle}}},
		{name: "zero member ID", change: dynamicConfChange(addLearnerOperation, confChangeSingle(0, pb.ConfChangeAddLearnerNode))},
		{name: "unsupported type", change: dynamicConfChange(addLearnerOperation, &pb.ConfChangeSingle{NodeId: uint64Ptr(4), Type: &unknownType})},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, _, err := validateDynamicConfChange(testCase.change); err == nil {
				t.Fatal("validateDynamicConfChange accepted malformed change")
			}
		})
	}
}

func TestValidateDynamicConfStateTransitionRejectsInvalidMembership(t *testing.T) {
	stable := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	joint := &pb.ConfState{Voters: []uint64{1}, VotersOutgoing: []uint64{2}}
	learnersNext := &pb.ConfState{Voters: []uint64{1}, LearnersNext: []uint64{2}}
	cases := []struct {
		name   string
		state  *pb.ConfState
		action MembershipChangeAction
		member NodeID
	}{
		{name: "add existing voter", state: stable, action: MembershipAddLearner, member: 1},
		{name: "add existing learner", state: stable, action: MembershipAddLearner, member: 3},
		{name: "promote non-learner", state: stable, action: MembershipPromoteVoter, member: 2},
		{name: "remove absent member", state: stable, action: MembershipRemove, member: 4},
		{name: "joint voter state", state: joint, action: MembershipAddLearner, member: 4},
		{name: "next learner state", state: learnersNext, action: MembershipAddLearner, member: 4},
		{name: "nil state", action: MembershipAddLearner, member: 4},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := validateDynamicConfStateTransition(testCase.state, testCase.action, testCase.member); err == nil {
				t.Fatal("validateDynamicConfStateTransition accepted invalid transition")
			}
		})
	}
}

func TestDynamicMalformedCommittedWALFailsReplay(t *testing.T) {
	root := t.TempDir()
	config := dynamicTestConfig(1, filepath.Join(root, "node-1"))
	directory := config.Directory
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		nodeConfig := dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID)))
		node, err := Open(nodeConfig)
		if err != nil {
			t.Fatalf("open node %d: %v", nodeID, err)
		}
		nodes[nodeID] = node
	}
	closeNodes := func() {
		for nodeID, node := range nodes {
			if node == nil {
				continue
			}
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", nodeID, err)
			}
			nodes[nodeID] = nil
		}
	}
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
		t.Fatal(err)
	}
	pump()
	if err := nodes[1].SnapshotWithApplicationData([]byte("snapshot")); err != nil {
		t.Fatalf("persist baseline snapshot: %v", err)
	}
	closeNodes()

	store, err := openDynamicStableStore(directory)
	if err != nil {
		t.Fatalf("open baseline WAL: %v", err)
	}
	lastIndex, err := store.memory.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	index := lastIndex + 1
	term := store.hardState.GetTerm()
	if term == 0 {
		term = 1
	}
	ready := etcdraft.Ready{
		Entries:   []*pb.Entry{{Index: uint64Ptr(index), Term: uint64Ptr(term), Type: pb.EntryConfChangeV2.Enum(), Data: mustMarshal(t, dynamicConfChange(addLearnerOperation, confChangeSingle(1, pb.ConfChangeAddLearnerNode)))}},
		HardState: &pb.HardState{Term: uint64Ptr(term), Vote: uint64Ptr(store.hardState.GetVote()), Commit: uint64Ptr(index)},
	}
	if err := store.appendReady(ready); err != nil {
		t.Fatalf("persist malformed committed entry: %v", err)
	}
	if err := store.close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(config)
	if reopened != nil {
		_ = reopened.Close()
	}
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("replay malformed committed transition error = %v, want ErrCorruptWAL", err)
	}
}

func dynamicConfChange(operationID string, changes ...*pb.ConfChangeSingle) *pb.ConfChangeV2 {
	return &pb.ConfChangeV2{Context: []byte(operationID), Changes: changes}
}
