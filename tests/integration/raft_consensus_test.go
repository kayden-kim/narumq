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
	"narumq/internal/application/takeover"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type messageRoute struct {
	from raftadapter.NodeID
	to   raftadapter.NodeID
}

type deterministicCluster struct {
	t        *testing.T
	dirs     map[raftadapter.NodeID]string
	nodes    map[raftadapter.NodeID]*raftadapter.Node
	active   map[raftadapter.NodeID]bool
	blocked  map[messageRoute]bool
	queue    []raftadapter.MessageDTO
	onCommit map[raftadapter.NodeID]func([]raftadapter.CommitDTO)
}

func newDeterministicCluster(t *testing.T) *deterministicCluster {
	t.Helper()
	root := t.TempDir()
	cluster := &deterministicCluster{
		t:        t,
		dirs:     make(map[raftadapter.NodeID]string, 3),
		nodes:    make(map[raftadapter.NodeID]*raftadapter.Node, 3),
		active:   make(map[raftadapter.NodeID]bool, 3),
		blocked:  make(map[messageRoute]bool),
		onCommit: make(map[raftadapter.NodeID]func([]raftadapter.CommitDTO)),
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
			if len(batch.Commits) != 0 {
				if callback, ok := c.onCommit[nodeID]; ok && callback != nil {
					callback(batch.Commits)
				}
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
	commits := c.nodes[nodeID].AppliedCommits()
	result := make([]string, len(commits))
	for index, commit := range commits {
		result[index] = string(commit.Data)
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
		commitCopy := cluster.nodes[2].AppliedCommits()
		commitCopy[0].Data[0] = 'X'
		if got := cluster.commands(2); !slices.Equal(got, []string{"alpha"}) {
			t.Fatalf("mutating AppliedCommits result changed node state: %v", got)
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

// Traceability: PRD CLU-04/05/10; ADR-0002 §7; ADR-0004 §8; cluster spec §2/2.1/11.2.
func TestReplicatedSessionOwnership(t *testing.T) {
	t.Run("concurrent acquire elects single owner", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		machines := map[raftadapter.NodeID]*takeover.StateMachine{
			1: takeover.NewStateMachine(),
			2: takeover.NewStateMachine(),
			3: takeover.NewStateMachine(),
		}
		for nodeID, sm := range machines {
			id := nodeID
			targetMachine := sm
			cluster.onCommit[id] = func(commits []raftadapter.CommitDTO) {
				if _, err := raftadapter.ApplyOwnershipCommits(targetMachine, commits); err != nil {
					t.Fatalf("node %d ApplyOwnershipCommits failed: %v", id, err)
				}
			}
		}

		cluster.campaign(1)

		tenantID, err := tenancy.NewTenantID("tenant-alpha")
		if err != nil {
			t.Fatal(err)
		}
		sessionKey, err := tenancy.NewSessionKey(tenantID, "client-shared")
		if err != nil {
			t.Fatal(err)
		}

		cmdA, err := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 0,
			OwnerNodeID:   "node-1",
		})
		if err != nil {
			t.Fatal(err)
		}
		cmdB, err := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 0,
			OwnerNodeID:   "node-2",
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := cluster.nodes[1].Propose(cmdA); err != nil {
			t.Fatal(err)
		}
		if err := cluster.nodes[1].Propose(cmdB); err != nil {
			t.Fatal(err)
		}
		cluster.pump()

		for _, nodeID := range []raftadapter.NodeID{1, 2, 3} {
			sm := machines[nodeID]
			owner, exists := sm.Ownership(sessionKey)
			if !exists {
				t.Fatalf("node %d: session ownership missing", nodeID)
			}
			if owner.OwnerNodeID() != "node-1" {
				t.Fatalf("node %d: OwnerNodeID = %q, want node-1", nodeID, owner.OwnerNodeID())
			}
			if owner.Epoch() != 1 {
				t.Fatalf("node %d: Epoch = %d, want 1", nodeID, owner.Epoch())
			}

			if err := sm.Authorize(sessionKey, "node-1", 1); err != nil {
				t.Fatalf("node %d: Authorize(node-1, 1) failed: %v", nodeID, err)
			}
			if err := sm.Authorize(sessionKey, "node-2", 1); !errors.Is(err, session.ErrInvalidOwnership) {
				t.Fatalf("node %d: Authorize(node-2, 1) error = %v, want ErrInvalidOwnership", nodeID, err)
			}
		}
	})

	t.Run("minority cannot acquire or advance epoch", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		machines := map[raftadapter.NodeID]*takeover.StateMachine{
			1: takeover.NewStateMachine(),
			2: takeover.NewStateMachine(),
			3: takeover.NewStateMachine(),
		}
		for nodeID, sm := range machines {
			id := nodeID
			targetMachine := sm
			cluster.onCommit[id] = func(commits []raftadapter.CommitDTO) {
				if _, err := raftadapter.ApplyOwnershipCommits(targetMachine, commits); err != nil {
					t.Fatalf("node %d ApplyOwnershipCommits failed: %v", id, err)
				}
			}
		}

		cluster.campaign(1)

		tenantID, _ := tenancy.NewTenantID("tenant-alpha")
		sessionKey, _ := tenancy.NewSessionKey(tenantID, "client-fenced")

		cmd1, err := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 0,
			OwnerNodeID:   "node-1",
		})
		if err != nil {
			t.Fatal(err)
		}
		cluster.propose(1, cmd1)

		for _, nodeID := range []raftadapter.NodeID{1, 2, 3} {
			owner, _ := machines[nodeID].Ownership(sessionKey)
			if owner.Epoch() != 1 || owner.OwnerNodeID() != "node-1" {
				t.Fatalf("node %d initial owner = %+v, want epoch 1 node-1", nodeID, owner)
			}
		}

		for _, peer := range []raftadapter.NodeID{2, 3} {
			cluster.blocked[messageRoute{from: 1, to: peer}] = true
			cluster.blocked[messageRoute{from: peer, to: 1}] = true
		}

		minorityCmd, _ := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 1,
			OwnerNodeID:   "node-1",
		})
		_ = cluster.nodes[1].Propose(minorityCmd)
		cluster.pump()

		owner1, _ := machines[1].Ownership(sessionKey)
		if owner1.Epoch() != 1 {
			t.Fatalf("minority node 1 advanced epoch to %d without quorum", owner1.Epoch())
		}

		for _, nodeID := range []raftadapter.NodeID{2, 3} {
			if err := cluster.nodes[nodeID].ForgetLeader(); err != nil {
				t.Fatal(err)
			}
		}
		cluster.pump()
		cluster.campaign(2)

		takeoverCmd, err := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 1,
			OwnerNodeID:   "node-2",
		})
		if err != nil {
			t.Fatal(err)
		}
		cluster.propose(2, takeoverCmd)

		for _, nodeID := range []raftadapter.NodeID{2, 3} {
			owner, exists := machines[nodeID].Ownership(sessionKey)
			if !exists || owner.Epoch() != 2 || owner.OwnerNodeID() != "node-2" {
				t.Fatalf("majority node %d owner = %+v, want epoch 2 node-2", nodeID, owner)
			}
		}
		owner1, _ = machines[1].Ownership(sessionKey)
		if owner1.Epoch() != 1 {
			t.Fatalf("isolated node 1 owner = %+v, want epoch 1", owner1)
		}

		for _, peer := range []raftadapter.NodeID{2, 3} {
			cluster.blocked[messageRoute{from: 1, to: peer}] = false
			cluster.blocked[messageRoute{from: peer, to: 1}] = false
		}
		cluster.tickActive(5)

		for _, nodeID := range []raftadapter.NodeID{1, 2, 3} {
			sm := machines[nodeID]
			owner, exists := sm.Ownership(sessionKey)
			if !exists || owner.Epoch() != 2 || owner.OwnerNodeID() != "node-2" {
				t.Fatalf("converged node %d owner = %+v, want epoch 2 node-2", nodeID, owner)
			}

			if err := sm.Authorize(sessionKey, "node-1", 1); !errors.Is(err, session.ErrStaleEpoch) {
				t.Fatalf("node %d Authorize(node-1, 1) error = %v, want ErrStaleEpoch", nodeID, err)
			}
			if err := sm.Authorize(sessionKey, "node-2", 2); err != nil {
				t.Fatalf("node %d Authorize(node-2, 2) failed: %v", nodeID, err)
			}
		}
	})

	t.Run("restart recovers ownership epoch and commit index", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		machines := map[raftadapter.NodeID]*takeover.StateMachine{
			1: takeover.NewStateMachine(),
			2: takeover.NewStateMachine(),
			3: takeover.NewStateMachine(),
		}
		for nodeID, sm := range machines {
			id := nodeID
			targetMachine := sm
			cluster.onCommit[id] = func(commits []raftadapter.CommitDTO) {
				if _, err := raftadapter.ApplyOwnershipCommits(targetMachine, commits); err != nil {
					t.Fatalf("node %d ApplyOwnershipCommits failed: %v", id, err)
				}
			}
		}

		cluster.campaign(1)

		tenantID, _ := tenancy.NewTenantID("tenant-alpha")
		sessionKey, _ := tenancy.NewSessionKey(tenantID, "client-recover")

		cmd1, _ := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 0,
			OwnerNodeID:   "node-1",
		})
		cluster.propose(1, cmd1)

		if err := cluster.nodes[2].Snapshot(); err != nil {
			t.Fatalf("node 2 snapshot failed: %v", err)
		}
		snapshotIndex := cluster.nodes[2].Status().AppliedIndex

		cmd2, _ := raftadapter.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 1,
			OwnerNodeID:   "node-2",
		})
		cluster.propose(1, cmd2)

		preRestartOwner, ok := machines[2].Ownership(sessionKey)
		if !ok || preRestartOwner.Epoch() != 2 {
			t.Fatalf("pre-restart node 2 owner = %+v, want epoch 2", preRestartOwner)
		}
		preRestartCommitIndex := preRestartOwner.CommitIndex()
		if preRestartCommitIndex <= snapshotIndex {
			t.Fatalf("pre-restart commit index %d <= snapshot index %d", preRestartCommitIndex, snapshotIndex)
		}

		if err := cluster.nodes[2].Close(); err != nil {
			t.Fatal(err)
		}
		cluster.active[2] = false

		reopenedNode, err := raftadapter.Open(raftadapter.Config{
			NodeID:    2,
			PeerIDs:   []raftadapter.NodeID{1, 2, 3},
			Directory: cluster.dirs[2],
		})
		if err != nil {
			t.Fatalf("reopen node 2 failed: %v", err)
		}
		defer reopenedNode.Close()

		recoveredMachine := takeover.NewStateMachine()
		appliedCommits := reopenedNode.AppliedCommits()
		results, err := raftadapter.ApplyOwnershipCommits(recoveredMachine, appliedCommits)
		if err != nil {
			t.Fatalf("replaying recovered commits failed: %v", err)
		}
		if len(results) == 0 {
			t.Fatal("no commits replayed into recovered state machine")
		}

		recoveredOwner, ok := recoveredMachine.Ownership(sessionKey)
		if !ok {
			t.Fatal("recovered ownership missing for sessionKey")
		}
		if recoveredOwner.OwnerNodeID() != preRestartOwner.OwnerNodeID() {
			t.Fatalf("recovered OwnerNodeID = %q, want %q", recoveredOwner.OwnerNodeID(), preRestartOwner.OwnerNodeID())
		}
		if recoveredOwner.Epoch() != preRestartOwner.Epoch() {
			t.Fatalf("recovered Epoch = %d, want %d", recoveredOwner.Epoch(), preRestartOwner.Epoch())
		}
		if recoveredOwner.CommitIndex() != preRestartOwner.CommitIndex() {
			t.Fatalf("recovered CommitIndex = %d, want %d", recoveredOwner.CommitIndex(), preRestartOwner.CommitIndex())
		}

		if err := recoveredMachine.Authorize(sessionKey, "node-2", 2); err != nil {
			t.Fatalf("recovered Authorize(node-2, 2) failed: %v", err)
		}
		if err := recoveredMachine.Authorize(sessionKey, "node-1", 1); !errors.Is(err, session.ErrStaleEpoch) {
			t.Fatalf("recovered Authorize(node-1, 1) error = %v, want ErrStaleEpoch", err)
		}
	})
}
