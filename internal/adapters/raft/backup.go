package raft

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	etcdraft "go.etcd.io/raft/v3"
)

// ErrNoApplicationSnapshot means no durable typed snapshot exists yet.
var ErrNoApplicationSnapshot = errors.New("raft node has no application snapshot")

// BackupImageDTO is a consistent copy of the latest durable typed snapshot and committed tail.
type BackupImageDTO struct {
	SnapshotIndex        uint64
	SnapshotTerm         uint64
	SnapshotData         []byte
	Commands             []AppliedCommandDTO
	ConfigurationChanges []CommittedConfigurationDTO
	CommitIndex          uint64
	CommitTerm           uint64
	VoterIDs             []NodeID
	LearnerIDs           []NodeID
}

// BackupImage copies the latest typed snapshot and committed records under one node lock.
func (n *Node) BackupImage() (BackupImageDTO, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return BackupImageDTO{}, err
	}
	if !n.dynamic {
		return BackupImageDTO{}, ErrConfigMismatch
	}
	if n.applying != nil || n.appliedIndex > n.store.hardState.GetCommit() {
		return BackupImageDTO{}, ErrBackpressure
	}
	snapshot, err := n.store.memory.Snapshot()
	if err != nil {
		return BackupImageDTO{}, fmt.Errorf("read backup snapshot: %w", err)
	}
	if etcdraft.IsEmptySnap(snapshot) || len(n.store.snapshotData) == 0 {
		return BackupImageDTO{}, ErrNoApplicationSnapshot
	}
	if len(n.appliedCommands) != len(n.commandIndexes) {
		return BackupImageDTO{}, errors.New("raft backup command indexes do not match applied commands")
	}
	commands := make([]AppliedCommandDTO, len(n.appliedCommands))
	for index, command := range n.appliedCommands {
		commands[index] = AppliedCommandDTO{Index: n.commandIndexes[index], Data: bytes.Clone(command)}
	}
	configurationChanges := slices.Clone(n.configurationChanges)
	commitIndex := n.appliedIndex
	commitTerm, err := n.store.memory.Term(commitIndex)
	if err != nil {
		return BackupImageDTO{}, fmt.Errorf("read backup commit term: %w", err)
	}
	voterIDs := nodeIDs(n.confState.GetVoters())
	learnerIDs := nodeIDs(n.confState.GetLearners())
	slices.Sort(voterIDs)
	slices.Sort(learnerIDs)
	return BackupImageDTO{
		SnapshotIndex:        snapshot.GetMetadata().GetIndex(),
		SnapshotTerm:         snapshot.GetMetadata().GetTerm(),
		SnapshotData:         bytes.Clone(n.store.snapshotData),
		Commands:             commands,
		ConfigurationChanges: configurationChanges,
		CommitIndex:          commitIndex,
		CommitTerm:           commitTerm,
		VoterIDs:             voterIDs,
		LearnerIDs:           learnerIDs,
	}, nil
}
