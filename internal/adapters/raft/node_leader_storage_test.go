package raft

import (
	"bytes"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-115
func TestNodeLeaderReplicationWaitsForLocalWAL(t *testing.T) {
	node := storageBoundaryNode(t)
	if err := node.Campaign(); err != nil {
		t.Fatal(err)
	}
	if _, err := processReadyForTest(t, node); err != nil {
		t.Fatal(err)
	}
	for _, responseType := range []pb.MessageType{pb.MsgPreVoteResp, pb.MsgVoteResp} {
		term := node.Status().Term
		if responseType == pb.MsgPreVoteResp {
			term++
		}
		vote := &pb.Message{Type: responseType.Enum(), From: new(uint64(1)), To: new(uint64(3)), Term: new(term)}
		if _, err := node.Step(MessageDTO{From: 1, To: 3, Data: mustMarshal(t, vote)}); err != nil {
			t.Fatal(err)
		}
		batch, err := processReadyForTest(t, node)
		if err != nil {
			t.Fatal(err)
		}
		for _, message := range batch.Messages {
			decoded := new(pb.Message)
			if err := proto.Unmarshal(message.Data, decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.GetType() != pb.MsgApp || len(decoded.Entries) == 0 {
				continue
			}
			index := decoded.Entries[len(decoded.Entries)-1].GetIndex()
			response := &pb.Message{Type: pb.MsgAppResp.Enum(), From: new(uint64(message.To)), To: new(uint64(3)), Term: decoded.Term, Index: new(index)}
			if _, err := node.Step(MessageDTO{From: message.To, To: 3, Data: mustMarshal(t, response)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := processReadyForTest(t, node); err != nil {
		t.Fatal(err)
	}
	entered, release := holdWALSync(t, node)
	payload := []byte("leader-must-persist-before-replication")
	if err := node.Propose(payload); err != nil {
		t.Fatal(err)
	}
	batch, err := node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	waitStorageBoundary(t, entered)
	for _, message := range batch.Messages {
		decoded := new(pb.Message)
		if err := proto.Unmarshal(message.Data, decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.GetType() == pb.MsgApp && len(decoded.Entries) != 0 {
			t.Fatal("leader replicated entries before its WAL sync")
		}
	}
	if err := node.Tick(); err != nil {
		t.Fatal(err)
	}
	batch, err = node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	heartbeatPeers := make(map[NodeID]bool)
	for _, message := range batch.Messages {
		decoded := new(pb.Message)
		if err := proto.Unmarshal(message.Data, decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.GetType() == pb.MsgApp && len(decoded.Entries) != 0 {
			t.Fatal("tick released entry replication before WAL sync")
		}
		if decoded.GetType() == pb.MsgHeartbeat {
			heartbeatPeers[message.To] = true
		}
	}
	if !heartbeatPeers[1] || !heartbeatPeers[2] {
		t.Fatalf("held WAL blocked heartbeats: %v", heartbeatPeers)
	}
	release()
	batch, err = processReadyForTest(t, node)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[NodeID]bool)
	for _, message := range batch.Messages {
		decoded := new(pb.Message)
		if err := proto.Unmarshal(message.Data, decoded); err != nil {
			t.Fatal(err)
		}
		for _, entry := range decoded.Entries {
			if bytes.Equal(entry.Data, payload) {
				seen[message.To] = true
			}
		}
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("durable leader proposal not replicated to both peers: %v", seen)
	}
}
