package integration

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"

	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	raftadapter "narumq/internal/adapters/raft"
	"narumq/internal/application/takeover"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/session"
	"narumq/internal/domain/tenancy"
)

type messageRoute struct {
	from raftadapter.NodeID
	to   raftadapter.NodeID
}

type deterministicCluster struct {
	t          *testing.T
	dirs       map[raftadapter.NodeID]string
	nodes      map[raftadapter.NodeID]*raftadapter.Node
	active     map[raftadapter.NodeID]bool
	blocked    map[messageRoute]bool
	queue      []raftadapter.MessageDTO
	onCommands map[raftadapter.NodeID]func([][]byte)
	onMessages func([]raftadapter.MessageDTO)
}

func newDeterministicCluster(t *testing.T) *deterministicCluster {
	t.Helper()
	root := t.TempDir()
	cluster := &deterministicCluster{
		t:          t,
		dirs:       make(map[raftadapter.NodeID]string, 3),
		nodes:      make(map[raftadapter.NodeID]*raftadapter.Node, 3),
		active:     make(map[raftadapter.NodeID]bool, 3),
		blocked:    make(map[messageRoute]bool),
		onCommands: make(map[raftadapter.NodeID]func([][]byte)),
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
			batch, err := processReadyForTest(c.t, c.nodes[nodeID])
			if err != nil {
				c.t.Fatalf("node %d ProcessReady(): %v", nodeID, err)
			}
			if len(batch.Messages) != 0 || len(batch.Commands) != 0 {
				progress = true
			}
			if len(batch.Commands) != 0 {
				if callback, ok := c.onCommands[nodeID]; ok && callback != nil {
					callback(batch.Commands)
				}
			}
			if c.onMessages != nil && len(batch.Messages) != 0 {
				c.onMessages(batch.Messages)
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
				if _, err := c.nodes[route.to].Step(message); err != nil {
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
	t.Run("snapshot compaction replaces WAL and prunes older snapshots", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		cluster.campaign(1)
		cluster.propose(1, []byte("alpha"))
		if err := cluster.nodes[2].Snapshot(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(cluster.dirs[2], "wal.bin")
		before := clusterWALLifecycleFileInfo(t, path)
		cluster.propose(1, []byte("beta"))
		if err := cluster.nodes[2].Snapshot(); err != nil {
			t.Fatal(err)
		}
		files, err := filepath.Glob(filepath.Join(cluster.dirs[2], "snapshot-*.bin"))
		want := fmt.Sprintf("snapshot-%020d.bin", cluster.nodes[2].Status().AppliedIndex)
		if err != nil || len(files) != 1 || filepath.Base(files[0]) != want {
			t.Errorf("snapshot files=%v error=%v, want only %s", files, err, want)
		}
		after := clusterWALLifecycleFileInfo(t, path)
		if os.SameFile(before, after) {
			t.Fatal("snapshot did not replace WAL")
		}
		cluster.propose(1, []byte("gamma"))
		if err := cluster.nodes[2].Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := raftadapter.Open(raftadapter.Config{NodeID: 2, PeerIDs: []raftadapter.NodeID{1, 2, 3}, Directory: cluster.dirs[2]})
		if err != nil {
			t.Fatal(err)
		}
		cluster.nodes[2] = reopened
		assertCommands(t, cluster, []raftadapter.NodeID{2}, []string{"alpha", "beta", "gamma"})
		if status := reopened.Status(); status.CommitIndex != status.AppliedIndex {
			t.Fatalf("recovered status=%+v", status)
		}
	})
	t.Run("leader failure preserves committed state", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		cluster.campaign(1)
		command := []byte("alpha")
		if err := cluster.nodes[1].Propose(command); err != nil {
			t.Fatal(err)
		}
		command[0] = 'X'
		batch, err := processReadyForTest(t, cluster.nodes[1])
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

	t.Run("slow follower keeps outbound append window bounded", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		cluster.campaign(1)
		cluster.blocked[messageRoute{from: 1, to: 3}] = true
		cluster.blocked[messageRoute{from: 3, to: 1}] = true
		cluster.blocked[messageRoute{from: 2, to: 3}] = true
		cluster.blocked[messageRoute{from: 3, to: 2}] = true

		const maxInflightMessages = 16
		inflightAppendRanges := make(map[[2]uint64]struct{}, maxInflightMessages)
		cluster.onMessages = func(messages []raftadapter.MessageDTO) {
			for _, message := range messages {
				if message.From != 1 || message.To != 3 {
					continue
				}
				var decoded raftpb.Message
				if err := proto.Unmarshal(message.Data, &decoded); err != nil {
					t.Fatalf("decode outbound message 1 -> 3: %v", err)
				}
				if decoded.GetType() != raftpb.MsgApp {
					continue
				}
				entries := decoded.GetEntries()
				if len(entries) == 0 {
					continue
				}
				firstIndex := decoded.GetIndex() + 1
				lastIndex := entries[len(entries)-1].GetIndex()
				inflightAppendRanges[[2]uint64{firstIndex, lastIndex}] = struct{}{}
			}
		}

		expectedCommands := make([]string, 0, 64)
		for index := range 64 {
			prefix := []byte(fmt.Sprintf("sequence-%02d", index))
			command := append(prefix, bytes.Repeat([]byte{'x'}, 1024-len(prefix))...)
			expectedCommands = append(expectedCommands, string(command))
			if err := cluster.nodes[1].Propose(command); err != nil {
				t.Fatalf("proposal %d: %v", index, err)
			}
			cluster.pump()
			assertCommands(t, cluster, []raftadapter.NodeID{1, 2}, expectedCommands)
			if got := cluster.commands(3); len(got) != 0 {
				t.Fatalf("slow follower applied commands during partition: %v", got)
			}
			if got := len(inflightAppendRanges); got > maxInflightMessages {
				t.Fatalf("distinct unacknowledged MsgApp ranges to slow follower = %d, want <= %d", got, maxInflightMessages)
			}
		}
		if got := len(inflightAppendRanges); got == 0 {
			t.Fatal("leader sent no entry-bearing MsgApp ranges to slow follower")
		}
		cluster.onMessages = nil

		delete(cluster.blocked, messageRoute{from: 1, to: 3})
		delete(cluster.blocked, messageRoute{from: 3, to: 1})
		delete(cluster.blocked, messageRoute{from: 2, to: 3})
		delete(cluster.blocked, messageRoute{from: 3, to: 2})
		converged := false
		for range 20 {
			cluster.tickActive(1)
			if slices.Equal(cluster.commands(1), expectedCommands) &&
				slices.Equal(cluster.commands(2), expectedCommands) &&
				slices.Equal(cluster.commands(3), expectedCommands) {
				converged = true
				break
			}
		}
		if !converged {
			t.Fatalf("cluster did not converge after healing slow-follower routes: node1=%d node2=%d node3=%d commands",
				len(cluster.commands(1)), len(cluster.commands(2)), len(cluster.commands(3)))
		}
		assertCommands(t, cluster, []raftadapter.NodeID{1, 2, 3}, expectedCommands)
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
			cluster.onCommands[id] = func(commands [][]byte) {
				if _, err := takeover.ApplyOwnershipCommands(targetMachine, commands); err != nil {
					t.Fatalf("node %d ApplyOwnershipCommands failed: %v", id, err)
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

		cmdA, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 0,
			OwnerNodeID:   "node-1",
		})
		if err != nil {
			t.Fatal(err)
		}
		cmdB, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
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

	t.Run("lost commit response retry preserves owner and epoch", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		machines := map[raftadapter.NodeID]*takeover.StateMachine{
			1: takeover.NewStateMachine(),
			2: takeover.NewStateMachine(),
			3: takeover.NewStateMachine(),
		}
		resultsByNode := make(map[raftadapter.NodeID][]takeover.ApplyResultDTO, 3)
		for nodeID, sm := range machines {
			id := nodeID
			targetMachine := sm
			cluster.onCommands[id] = func(commands [][]byte) {
				results, err := takeover.ApplyOwnershipCommands(targetMachine, commands)
				if err != nil {
					t.Fatalf("node %d ApplyOwnershipCommands failed: %v", id, err)
				}
				resultsByNode[id] = append(resultsByNode[id], results...)
			}
		}

		cluster.campaign(1)

		tenantID, err := tenancy.NewTenantID("tenant-alpha")
		if err != nil {
			t.Fatal(err)
		}
		sessionKey, err := tenancy.NewSessionKey(tenantID, "client-retry")
		if err != nil {
			t.Fatal(err)
		}
		command, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 0,
			OwnerNodeID:   "node-1",
		})
		if err != nil {
			t.Fatal(err)
		}
		wantCommand := bytes.Clone(command)

		cluster.propose(1, command)
		cluster.propose(1, command)

		for _, nodeID := range []raftadapter.NodeID{1, 2, 3} {
			results := resultsByNode[nodeID]
			if len(results) != 2 {
				t.Fatalf("node %d apply results = %d, want 2", nodeID, len(results))
			}
			if !results[0].Applied || results[0].Ownership.OwnerNodeID() != "node-1" || results[0].Ownership.Epoch() != 1 || results[0].Ownership.CommitIndex() != 1 {
				t.Fatalf("node %d first result = %+v, want node-1 epoch 1 sequence 1", nodeID, results[0])
			}
			if results[1].Applied || !errors.Is(results[1].Err, session.ErrStaleEpoch) {
				t.Fatalf("node %d retry result = %+v, want stale-epoch rejection", nodeID, results[1])
			}

			machine := machines[nodeID]
			owner, exists := machine.Ownership(sessionKey)
			if !exists || owner.OwnerNodeID() != "node-1" || owner.Epoch() != 1 || owner.CommitIndex() != 1 {
				t.Fatalf("node %d ownership = %+v, want node-1 epoch 1 sequence 1", nodeID, owner)
			}
			if machine.LastAppliedIndex() != 2 {
				t.Fatalf("node %d LastAppliedIndex = %d, want 2", nodeID, machine.LastAppliedIndex())
			}
			if err := machine.Authorize(sessionKey, "node-1", 1); err != nil {
				t.Fatalf("node %d Authorize(node-1, 1) failed: %v", nodeID, err)
			}

			commands := cluster.nodes[nodeID].AppliedCommands()
			if len(commands) != 2 || !bytes.Equal(commands[0], wantCommand) || !bytes.Equal(commands[1], wantCommand) {
				t.Fatalf("node %d applied commands = %q, want two identical payloads %q", nodeID, commands, wantCommand)
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
			cluster.onCommands[id] = func(commands [][]byte) {
				if _, err := takeover.ApplyOwnershipCommands(targetMachine, commands); err != nil {
					t.Fatalf("node %d ApplyOwnershipCommands failed: %v", id, err)
				}
			}
		}

		cluster.campaign(1)

		tenantID, _ := tenancy.NewTenantID("tenant-alpha")
		sessionKey, _ := tenancy.NewSessionKey(tenantID, "client-fenced")

		cmd1, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
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

		minorityCmd, _ := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
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

		takeoverCmd, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
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

	t.Run("restart recovers ownership epoch and command sequence", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		machines := map[raftadapter.NodeID]*takeover.StateMachine{
			1: takeover.NewStateMachine(),
			2: takeover.NewStateMachine(),
			3: takeover.NewStateMachine(),
		}
		for nodeID, sm := range machines {
			id := nodeID
			targetMachine := sm
			cluster.onCommands[id] = func(commands [][]byte) {
				if _, err := takeover.ApplyOwnershipCommands(targetMachine, commands); err != nil {
					t.Fatalf("node %d ApplyOwnershipCommands failed: %v", id, err)
				}
			}
		}

		cluster.campaign(1)

		tenantID, _ := tenancy.NewTenantID("tenant-alpha")
		sessionKey, _ := tenancy.NewSessionKey(tenantID, "client-recover")

		cmd1, _ := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 0,
			OwnerNodeID:   "node-1",
		})
		cluster.propose(1, cmd1)

		if err := cluster.nodes[2].Snapshot(); err != nil {
			t.Fatalf("node 2 snapshot failed: %v", err)
		}

		cmd2, _ := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
			SessionKey:    sessionKey,
			ExpectedEpoch: 1,
			OwnerNodeID:   "node-2",
		})
		cluster.propose(1, cmd2)
		ownershipCommands := cluster.nodes[2].AppliedCommands()

		preRestartOwner, ok := machines[2].Ownership(sessionKey)
		if !ok || preRestartOwner.Epoch() != 2 {
			t.Fatalf("pre-restart node 2 owner = %+v, want epoch 2", preRestartOwner)
		}
		if preRestartOwner.CommitIndex() != uint64(len(ownershipCommands)) {
			t.Fatalf("pre-restart ownership sequence index = %d, want %d applied ownership commands", preRestartOwner.CommitIndex(), len(ownershipCommands))
		}
		if machines[2].LastAppliedIndex() != preRestartOwner.CommitIndex() {
			t.Fatalf("pre-restart machine sequence index = %d, want %d", machines[2].LastAppliedIndex(), preRestartOwner.CommitIndex())
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
		appliedCommands := reopenedNode.AppliedCommands()
		results, err := takeover.ApplyOwnershipCommands(recoveredMachine, appliedCommands)
		if err != nil {
			t.Fatalf("replaying recovered commands failed: %v", err)
		}
		if len(results) != len(appliedCommands) {
			t.Fatalf("replayed results = %d, want %d", len(results), len(appliedCommands))
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
			t.Fatalf("recovered ownership sequence index = %d, want %d", recoveredOwner.CommitIndex(), preRestartOwner.CommitIndex())
		}
		if recoveredMachine.LastAppliedIndex() != machines[2].LastAppliedIndex() {
			t.Fatalf("recovered machine sequence index = %d, want %d", recoveredMachine.LastAppliedIndex(), machines[2].LastAppliedIndex())
		}

		if err := recoveredMachine.Authorize(sessionKey, "node-2", 2); err != nil {
			t.Fatalf("recovered Authorize(node-2, 2) failed: %v", err)
		}
		if err := recoveredMachine.Authorize(sessionKey, "node-1", 1); !errors.Is(err, session.ErrStaleEpoch) {
			t.Fatalf("recovered Authorize(node-1, 1) error = %v, want ErrStaleEpoch", err)
		}
	})
	t.Run("stale owner session mutations are fenced after takeover", func(t *testing.T) {
		cluster := newDeterministicCluster(t)
		tenantID, err := tenancy.NewTenantID("tenant-alpha")
		if err != nil {
			t.Fatal(err)
		}
		key, err := tenancy.NewSessionKey(tenantID, "client-fenced-mutations")
		if err != nil {
			t.Fatal(err)
		}
		sessionID := routing.SessionID("client-fenced-mutations")
		type typedMutation struct {
			subscription *takeover.PersistentSubscriptionCommandDTO
			inflight     *takeover.QoSInflightCommandDTO
			outbound     bool
		}
		// Tokens exist only in this test: no persisted encoding for these DTOs is decided.
		mutations := make(map[string]typedMutation)
		sub := takeover.PersistentSubscriptionCommandDTO{SessionKey: key, OwnerNodeID: "node-1", Epoch: 1,
			Subscription: mqtt.Subscription{Filter: "orders/#", QoS: mqtt.QoSAtLeastOnce}}
		in := takeover.QoSInflightCommandDTO{SessionKey: key, OwnerNodeID: "node-1", Epoch: 1,
			Publish: mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 10, Topic: "orders/1"}}
		out := in
		out.Publish.PacketID = 20
		mutations["test/subscription/baseline"] = typedMutation{subscription: &sub}
		mutations["test/inbound/baseline"] = typedMutation{inflight: &in}
		mutations["test/outbound/baseline"] = typedMutation{inflight: &out, outbound: true}
		oldSub := sub
		oldSub.Subscription.QoS = mqtt.QoSExactlyOnce
		oldIn := in
		oldIn.Publish.PacketID = 11
		oldOut := out
		oldOut.Publish.PacketID = 21
		// Prepare the old-owner mutations before committing successor ownership.
		mutations["test/subscription/stale"] = typedMutation{subscription: &oldSub}
		mutations["test/inbound/stale"] = typedMutation{inflight: &oldIn}
		mutations["test/outbound/stale"] = typedMutation{inflight: &oldOut, outbound: true}

		machines := make(map[raftadapter.NodeID]*takeover.StateMachine, 3)
		routes := make(map[raftadapter.NodeID]*routing.Table, 3)
		resultsByNode := make(map[raftadapter.NodeID][]takeover.ApplyResultDTO, 3)
		for _, nodeID := range cluster.nodeIDs() {
			machine := takeover.NewStateMachine()
			machines[nodeID] = machine
			routes[nodeID] = routing.NewTable()
			id := nodeID
			cluster.onCommands[id] = func(commands [][]byte) {
				for _, payload := range commands {
					position := machine.LastAppliedIndex() + 1
					var result takeover.ApplyResultDTO
					if len(payload) != 0 && payload[0] == '{' {
						command, err := takeover.DecodeAcquireOwnershipCommand(payload)
						if err != nil {
							t.Fatalf("node %d decode acquire: %v", id, err)
						}
						result = machine.Apply(position, command)
					} else {
						mutation, ok := mutations[string(payload)]
						if !ok {
							t.Fatalf("node %d unknown test mutation %q", id, payload)
						}
						switch {
						case mutation.subscription != nil:
							result = machine.ApplyPersistentSubscription(position, *mutation.subscription)
						case mutation.inflight != nil && mutation.outbound:
							result = machine.ApplyOutboundQoSInflight(position, *mutation.inflight)
						case mutation.inflight != nil:
							result = machine.ApplyInboundQoSInflight(position, *mutation.inflight)
						default:
							t.Fatalf("node %d empty test mutation %q", id, payload)
						}
						if result.Applied && mutation.subscription != nil {
							table := routing.NewTable()
							if err := table.BindSessionTenant(sessionID, tenantID); err != nil {
								t.Fatal(err)
							}
							for _, subscription := range machine.PersistentSubscriptions(key) {
								if _, err := table.Subscribe(sessionID, subscription); err != nil {
									t.Fatal(err)
								}
							}
							routes[id] = table
						}
					}
					resultsByNode[id] = append(resultsByNode[id], result)
				}
			}
		}
		acquire := func(owner string, expected session.Epoch) []byte {
			t.Helper()
			payload, err := takeover.EncodeAcquireOwnershipCommand(takeover.AcquireOwnershipCommandDTO{
				SessionKey: key, OwnerNodeID: owner, ExpectedEpoch: expected,
			})
			if err != nil {
				t.Fatal(err)
			}
			return payload
		}
		proposals := [][]byte{
			acquire("node-1", 0),
			[]byte("test/subscription/baseline"),
			[]byte("test/inbound/baseline"),
			[]byte("test/outbound/baseline"),
			acquire("node-2", 1),
			[]byte("test/subscription/stale"),
			[]byte("test/inbound/stale"),
			[]byte("test/outbound/stale"),
		}
		cluster.campaign(1)
		for _, payload := range proposals[:5] {
			cluster.propose(1, payload)
		}
		type aggregateSnapshot struct {
			owner    session.Ownership
			subs     []mqtt.Subscription
			inbound  []mqtt.PacketIdentifier
			outbound []mqtt.PacketIdentifier
			lookup   mqtt.Subscription
			found    bool
			routes   []routing.SubscriptionRecord
		}
		snapshots := make(map[raftadapter.NodeID]aggregateSnapshot, 3)
		for _, nodeID := range cluster.nodeIDs() {
			machine := machines[nodeID]
			owner, exists := machine.Ownership(key)
			if !exists || owner.OwnerNodeID() != "node-2" || owner.Epoch() != 2 || owner.CommitIndex() != 5 || machine.LastAppliedIndex() != 5 {
				t.Fatalf("node %d takeover owner = %v exists %v sequence %d", nodeID, owner, exists, machine.LastAppliedIndex())
			}
			lookup, found := routes[nodeID].Lookup(sessionID, "orders/#")
			snapshots[nodeID] = aggregateSnapshot{owner, machine.PersistentSubscriptions(key), machine.InboundQoSInflight(key),
				machine.OutboundQoSInflight(key), lookup, found, routes[nodeID].Snapshot()}
		}
		for _, payload := range proposals[5:] {
			cluster.propose(1, payload)
		}
		for _, nodeID := range cluster.nodeIDs() {
			results := resultsByNode[nodeID]
			if len(results) != 8 {
				t.Fatalf("node %d results = %d, want 8", nodeID, len(results))
			}
			for position, result := range results {
				if position < 5 && (!result.Applied || result.Err != nil) {
					t.Fatalf("node %d position %d = %+v, want applied", nodeID, position+1, result)
				}
				if position >= 5 && (result.Applied || !errors.Is(result.Err, session.ErrStaleEpoch)) {
					t.Fatalf("node %d position %d = %+v, want stale epoch", nodeID, position+1, result)
				}
			}
			machine := machines[nodeID]
			owner, exists := machine.Ownership(key)
			lookup, found := routes[nodeID].Lookup(sessionID, "orders/#")
			got := aggregateSnapshot{owner, machine.PersistentSubscriptions(key), machine.InboundQoSInflight(key),
				machine.OutboundQoSInflight(key), lookup, found, routes[nodeID].Snapshot()}
			want := aggregateSnapshot{snapshots[nodeID].owner, []mqtt.Subscription{sub.Subscription},
				[]mqtt.PacketIdentifier{10}, []mqtt.PacketIdentifier{20}, sub.Subscription, true,
				[]routing.SubscriptionRecord{{TenantID: tenantID, SessionID: sessionID, TopicFilter: "orders/#", QoS: mqtt.QoSAtLeastOnce}}}
			if !exists || !reflect.DeepEqual(snapshots[nodeID], want) || !reflect.DeepEqual(got, want) ||
				machine.LastAppliedIndex() != 8 {
				t.Fatalf("node %d snapshot after takeover = %+v, after stale = %+v, want %+v; sequence %d", nodeID, snapshots[nodeID], got, want, machine.LastAppliedIndex())
			}
			committed := cluster.nodes[nodeID].AppliedCommands()
			if len(committed) != len(proposals) {
				t.Fatalf("node %d committed %d commands, want %d", nodeID, len(committed), len(proposals))
			}
			for index, command := range committed {
				if !bytes.Equal(command, proposals[index]) {
					t.Fatalf("node %d command %d = %q, want %q", nodeID, index+1, command, proposals[index])
				}
			}
		}
	})
}
