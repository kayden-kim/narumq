package raft

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
)

func TestFollowerReadIndexCompletesAfterQuorumResponse(t *testing.T) {
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
		for nodeID, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", nodeID, err)
			}
		}
	}()

	readStates := make(map[NodeID][]ReadStateDTO)
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
				readStates[nodeID] = append(readStates[nodeID], batch.ReadStates...)
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
	if got := nodes[1].Status().Role; got != RoleLeader {
		t.Fatalf("node 1 role after campaign = %v, want leader", got)
	}
	if got := nodes[2].Status().Role; got != RoleFollower {
		t.Fatalf("node 2 role after campaign = %v, want follower", got)
	}
	if err := nodes[1].Propose([]byte("committed-read-index-baseline")); err != nil {
		t.Fatalf("propose baseline: %v", err)
	}
	pump()
	committedIndex := nodes[2].Status().AppliedIndex
	if committedIndex == 0 {
		t.Fatal("baseline command was not applied before ReadIndex")
	}

	requestContext := []byte("follower-read-index-1")
	if err := nodes[2].ReadIndex(requestContext); err != nil {
		t.Fatalf("follower ReadIndex(): %v", err)
	}
	pump()
	states := readStates[2]
	if len(states) != 1 {
		t.Fatalf("follower ReadStates = %+v, want one quorum response", states)
	}
	if !bytes.Equal(states[0].RequestContext, requestContext) {
		t.Fatalf("ReadState context = %q, want %q", states[0].RequestContext, requestContext)
	}
	if states[0].Index < committedIndex || states[0].Index > nodes[2].Status().AppliedIndex {
		t.Fatalf("ReadState index = %d, applied=%d baseline=%d", states[0].Index, nodes[2].Status().AppliedIndex, committedIndex)
	}
	if !slices.Equal(nodes[2].Status().VoterIDs, []NodeID{1, 2, 3}) {
		t.Fatalf("follower voter set changed during ReadIndex: %v", nodes[2].Status().VoterIDs)
	}
}
