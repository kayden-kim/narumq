package integration

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	raftadapter "narumq/internal/adapters/raft"
)

type messageRoute struct {
	from raftadapter.NodeID
	to   raftadapter.NodeID
}

type deterministicCluster struct {
	t       *testing.T
	dirs    map[raftadapter.NodeID]string
	nodes   map[raftadapter.NodeID]*raftadapter.Node
	active  map[raftadapter.NodeID]bool
	blocked map[messageRoute]bool
	queue   []raftadapter.MessageDTO
}

func newDeterministicCluster(t *testing.T) *deterministicCluster {
	t.Helper()
	root := t.TempDir()
	cluster := &deterministicCluster{
		t:       t,
		dirs:    make(map[raftadapter.NodeID]string, 3),
		nodes:   make(map[raftadapter.NodeID]*raftadapter.Node, 3),
		active:  make(map[raftadapter.NodeID]bool, 3),
		blocked: make(map[messageRoute]bool),
	}
	for _, nodeID := range []raftadapter.NodeID{1, 2, 3} {
		directory := filepath.Join(root, fmt.Sprintf("node-%d", nodeID))
		node, err := raftadapter.Open(raftadapter.Config{
			NodeID:    nodeID,
			PeerIDs:   []raftadapter.NodeID{3, 1, 2},
			Directory: directory,
		})
		if err != nil {
			t.Fatal(err)
		}
		cluster.dirs[nodeID] = directory
		cluster.nodes[nodeID] = node
		cluster.active[nodeID] = true
	}
	cluster.pump()
	for _, nodeID := range []raftadapter.NodeID{1, 2, 3} {
		if !cluster.nodes[nodeID].Status().Bootstrapped {
			t.Fatalf("node %d did not bootstrap", nodeID)
		}
	}
	t.Cleanup(cluster.close)
	return cluster
}

func (c *deterministicCluster) close() {
	c.t.Helper()
	for _, nodeID := range c.nodeIDs() {
		if err := c.nodes[nodeID].Close(); err != nil && !errors.Is(err, raftadapter.ErrClosed) {
			c.t.Errorf("node %d Close(): %v", nodeID, err)
		}
	}
}

func (c *deterministicCluster) pump() {
	c.t.Helper()
	const maxRounds = 512
	for range maxRounds {
		progress := false
		for _, nodeID := range c.nodeIDs() {
			if !c.active[nodeID] {
				continue
			}
			batch, err := c.nodes[nodeID].ProcessReady()
			if err != nil {
				c.t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
			}
			if len(batch.Messages) != 0 || len(batch.Commits) != 0 {
				progress = true
			}
			c.queue = append(c.queue, batch.Messages...)
		}
		if len(c.queue) != 0 {
			pending := c.queue
			c.queue = nil
			for _, message := range pending {
				route := messageRoute{from: message.From, to: message.To}
				if !c.active[route.from] || !c.active[route.to] || c.blocked[route] {
					progress = true
					continue
				}
				if err := c.nodes[route.to].Step(message); err != nil {
					c.t.Fatalf("deliver %d -> %d: %v", route.from, route.to, err)
				}
				if len(message.Data) != 0 {
					message.Data[0] ^= 0xff
				}
				progress = true
			}
		}
		if !progress && len(c.queue) == 0 {
			return
		}
	}
	c.t.Fatalf("deterministic pump exceeded %d rounds", maxRounds)
}

func (c *deterministicCluster) tickActive(count int) {
	c.t.Helper()
	for range count {
		for _, nodeID := range c.nodeIDs() {
			if !c.active[nodeID] {
				continue
			}
			if err := c.nodes[nodeID].Tick(); err != nil {
				c.t.Fatalf("node %d Tick(): %v", nodeID, err)
			}
		}
		c.pump()
	}
}

func (c *deterministicCluster) campaign(nodeID raftadapter.NodeID) {
	c.t.Helper()
	if err := c.nodes[nodeID].Campaign(); err != nil {
		c.t.Fatalf("node %d Campaign(): %v", nodeID, err)
	}
	c.pump()
}

func (c *deterministicCluster) propose(nodeID raftadapter.NodeID, command []byte) {
	c.t.Helper()
	if err := c.nodes[nodeID].Propose(command); err != nil {
		c.t.Fatalf("node %d Propose(): %v", nodeID, err)
	}
	c.pump()
}

func (c *deterministicCluster) partitionAll() {
	for _, from := range c.nodeIDs() {
		for _, to := range c.nodeIDs() {
			if from != to {
				c.blocked[messageRoute{from: from, to: to}] = true
			}
		}
	}
}

func (c *deterministicCluster) nodeIDs() []raftadapter.NodeID {
	ids := make([]raftadapter.NodeID, 0, len(c.nodes))
	for nodeID := range c.nodes {
		ids = append(ids, nodeID)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	return ids
}

func (c *deterministicCluster) commands(nodeID raftadapter.NodeID) []string {
	commands := c.nodes[nodeID].AppliedCommands()
	result := make([]string, len(commands))
	for index, command := range commands {
		result[index] = string(command)
	}
	return result
}

func assertCommands(t *testing.T, cluster *deterministicCluster, nodeIDs []raftadapter.NodeID, want []string) {
	t.Helper()
	for _, nodeID := range nodeIDs {
		if got := cluster.commands(nodeID); !slices.Equal(got, want) {
			t.Errorf("node %d commands = %v, want %v", nodeID, got, want)
		}
	}
}

// Traceability: PRD CLU-01/02/06/07/09/10; ADR-0001 §7 gate 4; ADR-0002 §7; cluster spec §6/7/10/11.
func TestRaftConsensusSpike(t *testing.T) {
	t.Run("leader failure preserves committed state", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		cluster.campaign(1)
		command := []byte("alpha")
		if err := cluster.nodes[1].Propose(command); err != nil {
			t.Fatal(err)
		}
		command[0] = 'X'
		batch, err := cluster.nodes[1].ProcessReady()
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Messages) == 0 {
			t.Fatal("proposal did not produce outbound messages")
		}
		messages := slices.Clone(batch.Messages)
		for index := range messages {
			messages[index].Data = bytes.Clone(messages[index].Data)
		}
		batch.Messages[0].Data[0] ^= 0xff
		cluster.queue = append(cluster.queue, messages...)
		cluster.pump()
		assertCommands(t, cluster, []raftadapter.NodeID{1, 2, 3}, []string{"alpha"})
		commandCopy := cluster.nodes[2].AppliedCommands()
		commandCopy[0][0] = 'X'
		if got := cluster.commands(2); !slices.Equal(got, []string{"alpha"}) {
			t.Fatalf("mutating AppliedCommands result changed node state: %v", got)
		}
		oldTerm := cluster.nodes[1].Status().Term
		if err := cluster.nodes[1].Close(); err != nil {
			t.Fatal(err)
		}
		cluster.active[1] = false
		for _, nodeID := range []raftadapter.NodeID{2, 3} {
			if err := cluster.nodes[nodeID].ForgetLeader(); err != nil {
				t.Fatal(err)
			}
		}
		cluster.pump()
		cluster.campaign(2)
		cluster.propose(2, []byte("beta"))
		status := cluster.nodes[2].Status()
		if status.Role != raftadapter.RoleLeader || status.Term <= oldTerm {
			t.Fatalf("new leader status = %#v, old term = %d", status, oldTerm)
		}
		assertCommands(t, cluster, []raftadapter.NodeID{2, 3}, []string{"alpha", "beta"})
	})

	t.Run("quorum loss cannot commit", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		cluster.campaign(1)
		if err := cluster.nodes[1].Propose(nil); !errors.Is(err, raftadapter.ErrEmptyCommand) {
			t.Fatalf("empty leader proposal = %v, want ErrEmptyCommand", err)
		}
		if err := cluster.nodes[1].Propose(bytes.Repeat([]byte{'z'}, maxProposalBytes+1)); !errors.Is(err, raftadapter.ErrProposalTooLarge) {
			t.Fatalf("oversized leader proposal = %v, want ErrProposalTooLarge", err)
		}
		cluster.partitionAll()
		cluster.propose(1, []byte("gamma"))
		if got := cluster.commands(1); len(got) != 0 {
			t.Fatalf("isolated leader commands after gamma = %v, want no commit", got)
		}
		largeCommand := bytes.Repeat([]byte{'x'}, maxProposalBytes)
		backpressured := false
		for index := range 64 {
			err := cluster.nodes[1].Propose(largeCommand)
			if errors.Is(err, raftadapter.ErrBackpressure) {
				backpressured = true
				break
			}
			if err != nil {
				t.Fatalf("large proposal %d: %v", index, err)
			}
			cluster.pump()
		}
		if !backpressured {
			t.Fatal("isolated leader proposals never reached ErrBackpressure")
		}
		cluster.tickActive(20)
		for _, nodeID := range cluster.nodeIDs() {
			if got := cluster.commands(nodeID); slices.Contains(got, "gamma") {
				t.Fatalf("node %d applied speculative gamma: %v", nodeID, got)
			}
		}
		if cluster.nodes[1].Status().Role == raftadapter.RoleLeader {
			t.Fatalf("isolated old leader retained leadership: %#v", cluster.nodes[1].Status())
		}
	})

	t.Run("WAL replay restores commit", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		cluster.campaign(1)
		cluster.propose(1, []byte("alpha"))
		if err := cluster.nodes[2].Close(); err != nil {
			t.Fatal(err)
		}
		cluster.active[2] = false
		restarted, err := raftadapter.Open(raftadapter.Config{NodeID: 2, PeerIDs: []raftadapter.NodeID{1, 2, 3}, Directory: cluster.dirs[2]})
		if err != nil {
			t.Fatal(err)
		}
		cluster.nodes[2] = restarted
		if status := restarted.Status(); status.CommitIndex != status.AppliedIndex || !slices.Equal(cluster.commands(2), []string{"alpha"}) {
			t.Fatalf("restarted status = %#v, commands = %v", status, cluster.commands(2))
		}
		cluster.active[2] = true
		cluster.tickActive(2)
		cluster.propose(1, []byte("beta"))
		assertCommands(t, cluster, []raftadapter.NodeID{1, 2, 3}, []string{"alpha", "beta"})
	})

	t.Run("snapshot plus WAL tail restores state", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		cluster.campaign(1)
		cluster.propose(1, []byte("alpha"))
		cluster.propose(1, []byte("beta"))
		if err := cluster.nodes[2].Snapshot(); err != nil {
			t.Fatal(err)
		}
		snapshotIndex := cluster.nodes[2].Status().AppliedIndex
		cluster.propose(1, []byte("gamma"))
		if err := os.WriteFile(filepath.Join(cluster.dirs[2], fmt.Sprintf("snapshot-%020d.bin.tmp", snapshotIndex+1)), []byte("stale temporary snapshot"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := cluster.nodes[2].Close(); err != nil {
			t.Fatal(err)
		}
		cluster.active[2] = false
		restarted, err := raftadapter.Open(raftadapter.Config{NodeID: 2, PeerIDs: []raftadapter.NodeID{1, 2, 3}, Directory: cluster.dirs[2]})
		if err != nil {
			t.Fatal(err)
		}
		cluster.nodes[2] = restarted
		status := restarted.Status()
		if status.AppliedIndex <= snapshotIndex || status.CommitIndex != status.AppliedIndex {
			t.Fatalf("snapshot tail status = %#v, snapshot index = %d", status, snapshotIndex)
		}
		if got := cluster.commands(2); !slices.Equal(got, []string{"alpha", "beta", "gamma"}) {
			t.Fatalf("snapshot tail commands = %v", got)
		}
	})
}

const maxProposalBytes = 64 * 1024
