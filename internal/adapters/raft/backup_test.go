package raft

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
)

func TestSnapshotFinalizeHookObservesFileBoundaries(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "node-1")
	var boundaries []PersistenceBoundaryDTO
	var observations []string
	config := dynamicTestConfig(1, directory)
	config.PersistenceBoundaryHook = func(boundary PersistenceBoundaryDTO) {
		if !boundary.CreatesSnapshot {
			return
		}
		temporary, err := filepath.Glob(filepath.Join(directory, "snapshot-*.bin.tmp"))
		if err != nil {
			observations = append(observations, fmt.Sprintf("glob temporary snapshots: %v", err))
			return
		}
		finalized, err := filepath.Glob(filepath.Join(directory, "snapshot-*.bin"))
		if err != nil {
			observations = append(observations, fmt.Sprintf("glob finalized snapshots: %v", err))
			return
		}
		switch boundary.Stage {
		case PersistenceBeforeSnapshotFinalize:
			if len(temporary) != 1 || len(finalized) != 0 {
				observations = append(observations, fmt.Sprintf("before finalize: temporary=%v finalized=%v", temporary, finalized))
			}
		case PersistenceAfterSnapshotFinalize:
			if len(temporary) != 0 || len(finalized) != 1 {
				observations = append(observations, fmt.Sprintf("after finalize: temporary=%v finalized=%v", temporary, finalized))
			}
		default:
			observations = append(observations, fmt.Sprintf("unexpected local snapshot stage %d", boundary.Stage))
		}
		boundaries = append(boundaries, boundary)
	}

	nodes := openBackupTestNodes(t, root, config)
	pumpBackupTestNodes(t, nodes)

	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pumpBackupTestNodes(t, nodes)
	if err := nodes[1].Propose([]byte("snapshot-boundary")); err != nil {
		t.Fatal(err)
	}
	pumpBackupTestNodes(t, nodes)
	if err := nodes[1].SnapshotWithApplicationData([]byte("typed-snapshot")); err != nil {
		t.Fatal(err)
	}

	if len(observations) != 0 {
		t.Fatalf("snapshot file boundary observations: %v", observations)
	}
	if len(boundaries) != 2 {
		t.Fatalf("local snapshot boundaries = %+v, want before and after finalize", boundaries)
	}
	if boundaries[0].Stage != PersistenceBeforeSnapshotFinalize || boundaries[1].Stage != PersistenceAfterSnapshotFinalize {
		t.Fatalf("local snapshot boundary order = %+v", boundaries)
	}
	for _, boundary := range boundaries {
		if !boundary.CreatesSnapshot || len(boundary.ConfigurationActions) != 0 || boundary.InstallsSnapshot {
			t.Fatalf("local snapshot boundary = %+v", boundary)
		}
	}
}
func TestNodeBackupImageCapturesSnapshotAndCommittedTail(t *testing.T) {
	root := t.TempDir()
	nodes := openBackupTestNodes(t, root, dynamicTestConfig(1, filepath.Join(root, "node-1")))
	pumpBackupTestNodes(t, nodes)
	if _, err := nodes[1].BackupImage(); !errors.Is(err, ErrNoApplicationSnapshot) {
		t.Fatalf("BackupImage() before typed snapshot = %v, want ErrNoApplicationSnapshot", err)
	}
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pumpBackupTestNodes(t, nodes)
	if nodes[1].Status().Role != RoleLeader {
		t.Fatalf("node 1 role = %v, want leader", nodes[1].Status().Role)
	}
	for _, command := range [][]byte{[]byte("before-snapshot"), []byte("second-before-snapshot")} {
		if err := nodes[1].Propose(command); err != nil {
			t.Fatal(err)
		}
		pumpBackupTestNodes(t, nodes)
	}
	snapshotIndex := nodes[1].Status().AppliedIndex
	snapshotData := []byte("durable typed snapshot")
	if err := nodes[1].SnapshotWithApplicationData(snapshotData); err != nil {
		t.Fatal(err)
	}
	tailCommands := [][]byte{[]byte("tail-one"), []byte("tail-two")}
	for _, command := range tailCommands {
		if err := nodes[1].Propose(command); err != nil {
			t.Fatal(err)
		}
		pumpBackupTestNodes(t, nodes)
	}

	image, err := nodes[1].BackupImage()
	if err != nil {
		t.Fatal(err)
	}
	if image.SnapshotIndex != snapshotIndex || image.SnapshotTerm == 0 || !bytes.Equal(image.SnapshotData, snapshotData) {
		t.Fatalf("backup snapshot = index:%d term:%d data:%q", image.SnapshotIndex, image.SnapshotTerm, image.SnapshotData)
	}
	if image.CommitIndex != nodes[1].Status().AppliedIndex || image.CommitTerm == 0 || len(image.Commands) != len(tailCommands) {
		t.Fatalf("backup boundary = index:%d term:%d commands:%+v", image.CommitIndex, image.CommitTerm, image.Commands)
	}
	for index, command := range tailCommands {
		if !bytes.Equal(image.Commands[index].Data, command) || image.Commands[index].Index <= image.SnapshotIndex {
			t.Fatalf("backup command %d = %+v, want %q after snapshot", index, image.Commands[index], command)
		}
	}
	if !slices.Equal(image.VoterIDs, []NodeID{1, 2, 3}) || len(image.LearnerIDs) != 0 {
		t.Fatalf("backup membership = voters:%v learners:%v", image.VoterIDs, image.LearnerIDs)
	}
	image.SnapshotData[0] ^= 0xff
	image.Commands[0].Data[0] ^= 0xff
	image.VoterIDs[0] = 99
	independent, err := nodes[1].BackupImage()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(independent.SnapshotData, snapshotData) || !bytes.Equal(independent.Commands[0].Data, tailCommands[0]) || !slices.Equal(independent.VoterIDs, []NodeID{1, 2, 3}) {
		t.Fatalf("mutating backup image changed node state: %+v", independent)
	}
}

func openBackupTestNodes(t *testing.T, root string, firstConfig Config) map[NodeID]*Node {
	t.Helper()
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		config := dynamicTestConfig(nodeID, filepath.Join(root, fmt.Sprintf("node-%d", nodeID)))
		if nodeID == 1 {
			config = firstConfig
		}
		node, err := Open(config)
		if err != nil {
			for _, opened := range nodes {
				_ = opened.Close()
			}
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

func pumpBackupTestNodes(t *testing.T, nodes map[NodeID]*Node) {
	t.Helper()
	for range 512 {
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
