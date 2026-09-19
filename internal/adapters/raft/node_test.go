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
	commands := node.AppliedCommands()
	if len(commands) != 0 {
		t.Fatalf("bootstrap commands = %q, want empty", commands)
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
