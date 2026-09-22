package raft

import (
	"errors"
	"path/filepath"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestNodeConfigAndLifecycle(t *testing.T) {
	tests := []Config{
		{},
		{NodeID: 1, PeerIDs: []NodeID{1, 2}},
		{NodeID: 1, PeerIDs: []NodeID{1, 1, 2}},
		{NodeID: 1, PeerIDs: []NodeID{1, 2, 0}},
		{NodeID: 4, PeerIDs: []NodeID{1, 2, 3}},
	}
	for index, config := range tests {
		t.Run("invalid-"+string(rune('a'+index)), func(t *testing.T) {
			config.Directory = t.TempDir()
			_, err := Open(config)
			if !errors.Is(err, ErrConfigMismatch) {
				t.Fatalf("Open() error = %v, want ErrConfigMismatch", err)
			}
		})
	}

	directory := filepath.Join(t.TempDir(), "node")
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{3, 1, 2}, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Messages) != 0 || len(batch.Commits) != 0 {
		t.Fatalf("bootstrap batch = %#v, want no external messages or application commits", batch)
	}
	status := node.Status()
	if !status.Bootstrapped || status.AppliedIndex == 0 || status.CommitIndex != status.AppliedIndex {
		t.Fatalf("bootstrap status = %#v", status)
	}
	commits := node.AppliedCommits()
	if len(commits) != 0 {
		t.Fatalf("bootstrap commits = %#v, want empty", commits)
	}
	if err := node.Snapshot(); err != nil {
		t.Fatalf("same-index Snapshot() = %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.Tick(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Tick() after Close() = %v, want ErrClosed", err)
	}
	if _, err := node.ProcessReady(); !errors.Is(err, ErrClosed) {
		t.Fatalf("ProcessReady() after Close() = %v, want ErrClosed", err)
	}
}

func TestNodeMessageAndProposalValidation(t *testing.T) {
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: filepath.Join(t.TempDir(), "node")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	if err := node.Propose([]byte("command")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose() on follower = %v, want ErrNotLeader", err)
	}
	if err := node.Step(MessageDTO{From: 2, To: 1, Data: []byte{0xff}}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Step() malformed payload = %v, want ErrInvalidMessage", err)
	}
	message := &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(3))}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Step(MessageDTO{From: 2, To: 1, Data: data}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Step() metadata mismatch = %v, want ErrInvalidMessage", err)
	}
	if err := node.Propose(nil); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("empty proposal on follower = %v, want ErrNotLeader", err)
	}
}
func TestNodeMessageValidationAndFailureState(t *testing.T) {
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	for _, message := range []MessageDTO{
		{},
		{From: 1, To: 1, Data: []byte{1}},
		{From: 2, To: 2, Data: []byte{1}},
		{From: 2, To: 1, Data: make([]byte, maxMessageSize+1)},
	} {
		if err := node.Step(message); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("Step(%#v) error = %v, want ErrInvalidMessage", message, err)
		}
	}
	valid := &pb.Message{Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1)}
	data, err := proto.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Step(MessageDTO{From: 2, To: 1, Data: data}); err != nil && !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("valid peer Step() error = %v", err)
	}
	if err := node.ForgetLeader(); err != nil {
		t.Fatalf("ForgetLeader() error = %v", err)
	}
	node.fail(errors.New("injected failure"))
	if err := node.Tick(); !errors.Is(err, ErrFailed) {
		t.Fatalf("Tick() after failure = %v, want ErrFailed", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.ForgetLeader(); !errors.Is(err, ErrClosed) {
		t.Fatalf("ForgetLeader() after close = %v, want ErrClosed", err)
	}
}

func TestNodeHelpersCloneAndFixedConfiguration(t *testing.T) {
	if fixedConfState(nil, []NodeID{1, 2, 3}) {
		t.Fatal("fixedConfState(nil) unexpectedly true")
	}
	if fixedConfState(&pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}, []NodeID{1, 2, 3}) {
		t.Fatal("fixedConfState with learner unexpectedly true")
	}
	state := &pb.ConfState{Voters: []uint64{3, 1, 2}}
	if !fixedConfState(state, []NodeID{1, 2, 3}) {
		t.Fatal("fixedConfState rejected sorted-equivalent voters")
	}
	commits := []CommitDTO{{Index: 1, Data: []byte("one")}, {Index: 2, Data: nil}}
	clone := cloneCommits(commits)
	clone[0].Data[0] = 'X'
	if string(commits[0].Data) != "one" {
		t.Fatal("cloneCommits shared payload")
	}
	entries := cloneEntries([]*pb.Entry{{Index: uint64Ptr(1), Data: []byte("entry")}})
	entries[0].Data[0] = 'X'
	if string(entries[0].Data) == "entry" {
		t.Fatal("cloneEntries did not permit independent mutation")
	}
	if !errors.Is((&Node{}).fail(errors.New("failure")), ErrFailed) {
		t.Fatal("fail() did not wrap ErrFailed")
	}
}
