// Package raft contains the Phase 0 deterministic consensus adapter.
package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"sort"
	"sync"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	electionTick            = 10
	heartbeatTick           = 1
	maxSizePerMessage       = 64 * 1024
	maxCommittedReadySize   = 1 * 1024 * 1024
	maxUncommittedEntrySize = 1 * 1024 * 1024
	maxInflightMessages     = 16
	maxInflightBytes        = 1 * 1024 * 1024
	maxProposalSize         = 64 * 1024
	maxMessageSize          = 1 * 1024 * 1024
)

var (
	// ErrConfigMismatch means persisted cluster identity differs from Config.
	ErrConfigMismatch = errors.New("raft cluster configuration mismatch")
	// ErrBootstrapIncomplete means a directory contains an unfinished initial bootstrap.
	ErrBootstrapIncomplete = errors.New("raft bootstrap is incomplete")
	// ErrClosed means the node has already been closed.
	ErrClosed = errors.New("raft node is closed")
	// ErrFailed means a persistence or apply failure permanently stopped the node.
	ErrFailed = errors.New("raft node has failed")
	// ErrNotLeader means a proposal or campaign operation requires a leader.
	ErrNotLeader = errors.New("raft node is not leader")
	// ErrEmptyCommand means a proposal has no application bytes.
	ErrEmptyCommand = errors.New("raft proposal is empty")
	// ErrProposalTooLarge means a proposal exceeds the fixed spike limit.
	ErrProposalTooLarge = errors.New("raft proposal is too large")
	// ErrBackpressure means etcd/raft rejected a proposal at its uncommitted limit.
	ErrBackpressure = errors.New("raft proposal backpressure")
	// ErrInvalidMessage means a peer message failed the adapter boundary checks.
	ErrInvalidMessage = errors.New("invalid raft peer message")
	// ErrCorruptWAL means a WAL cannot be safely replayed.
	ErrCorruptWAL = errors.New("corrupt raft WAL")
	// ErrCorruptSnapshot means a finalized snapshot cannot be safely restored.
	ErrCorruptSnapshot = errors.New("corrupt raft snapshot")
)

var discardRaftLogger = &etcdraft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

// NodeID identifies a fixed voter in the Phase 0 cluster.
type NodeID uint64

// Role describes the externally visible role of a Node.
type Role uint8

const (
	// RoleFollower identifies a follower or pre-candidate node.
	RoleFollower Role = iota
	// RoleCandidate identifies a candidate node.
	RoleCandidate
	// RoleLeader identifies the elected leader.
	RoleLeader
)

// Config contains the fixed three-voter identity and local persistence directory.
type Config struct {
	NodeID    NodeID
	PeerIDs   []NodeID
	Directory string
}

// MessageDTO is the adapter-owned serialized peer message.
type MessageDTO struct {
	From NodeID
	To   NodeID
	Data []byte
}

// CommitDTO is an ordered application command that reached the local commit boundary.
type CommitDTO struct {
	Index uint64
	Data  []byte
}

// BatchDTO contains outbound peer messages and newly applied commands.
type BatchDTO struct {
	Messages []MessageDTO
	Commits  []CommitDTO
}

// StatusDTO reports the node's deterministic Raft and application progress.
type StatusDTO struct {
	NodeID       NodeID
	LeaderID     NodeID
	Role         Role
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
	Bootstrapped bool
}

// Node owns one thread-safe wrapper around the thread-unsafe etcd/raft RawNode.
// The wrapper never starts goroutines or reads wall-clock time; callers own ticks and delivery.
type Node struct {
	mu sync.Mutex

	id      NodeID
	peerIDs []NodeID
	store   *stableStore
	raw     *etcdraft.RawNode
	closed  bool
	failure error

	bootstrapped   bool
	confState      *pb.ConfState
	appliedIndex   uint64
	appliedCommits []CommitDTO
	onReadyEvent   func(string)
}

// Open opens a new or previously bootstrapped deterministic three-voter node.
func Open(config Config) (*Node, error) {
	peerIDs, err := normalizePeerIDs(config.NodeID, config.PeerIDs)
	if err != nil {
		return nil, err
	}
	isNew, err := prepareClusterDirectory(config.Directory, config.NodeID, peerIDs)
	if err != nil {
		return nil, err
	}
	store, err := openStableStore(config.Directory)
	if err != nil {
		return nil, err
	}

	raftConfig := &etcdraft.Config{
		ID:                        uint64(config.NodeID),
		ElectionTick:              electionTick,
		HeartbeatTick:             heartbeatTick,
		Storage:                   store.memory,
		Applied:                   store.appliedIndex,
		AsyncStorageWrites:        false,
		MaxSizePerMsg:             maxSizePerMessage,
		MaxCommittedSizePerReady:  maxCommittedReadySize,
		MaxUncommittedEntriesSize: maxUncommittedEntrySize,
		MaxInflightMsgs:           maxInflightMessages,
		MaxInflightBytes:          maxInflightBytes,
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: true,
		Logger:                    discardRaftLogger,
	}
	raw, err := etcdraft.NewRawNode(raftConfig)
	if err != nil {
		_ = store.close()
		return nil, fmt.Errorf("create raft node: %w", err)
	}
	if isNew {
		peers := make([]etcdraft.Peer, len(peerIDs))
		for index, peerID := range peerIDs {
			peers[index] = etcdraft.Peer{ID: uint64(peerID)}
		}
		if err := raw.Bootstrap(peers); err != nil {
			_ = store.close()
			return nil, fmt.Errorf("bootstrap raft node: %w", err)
		}
	}

	node := &Node{
		id:             config.NodeID,
		peerIDs:        slices.Clone(peerIDs),
		store:          store,
		raw:            raw,
		bootstrapped:   !isNew && fixedConfState(store.confState, peerIDs),
		confState:      cloneConfState(store.confState),
		appliedIndex:   store.appliedIndex,
		appliedCommits: cloneCommits(store.commits),
	}
	return node, nil
}

// Campaign asks the local RawNode to start an election.
func (n *Node) Campaign() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	return n.raw.Campaign()
}

// ForgetLeader clears a follower's current leader before a supervised failover campaign.
func (n *Node) ForgetLeader() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	return n.raw.ForgetLeader()
}

// Tick advances the logical Raft clock exactly once.
func (n *Node) Tick() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	n.raw.Tick()
	return nil
}

// Propose accepts a copied application command only while this node is leader.
func (n *Node) Propose(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if n.raw.Status().RaftState != etcdraft.StateLeader {
		return ErrNotLeader
	}
	if len(data) == 0 {
		return ErrEmptyCommand
	}
	if len(data) > maxProposalSize {
		return ErrProposalTooLarge
	}
	command := bytes.Clone(data)
	if err := n.raw.Propose(command); err != nil {
		if errors.Is(err, etcdraft.ErrProposalDropped) {
			return fmt.Errorf("%w: %v", ErrBackpressure, err)
		}
		return err
	}
	return nil
}

// Step validates and delivers one serialized peer message to the local RawNode.
func (n *Node) Step(message MessageDTO) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if message.From == 0 || message.To == 0 || message.To != n.id || len(message.Data) == 0 || len(message.Data) > maxMessageSize {
		return ErrInvalidMessage
	}
	payload := bytes.Clone(message.Data)
	var decoded pb.Message
	if err := proto.Unmarshal(payload, &decoded); err != nil {
		return fmt.Errorf("%w: decode message: %v", ErrInvalidMessage, err)
	}
	if decoded.GetFrom() != uint64(message.From) || decoded.GetTo() != uint64(message.To) || decoded.GetFrom() == 0 || decoded.GetTo() == 0 {
		return ErrInvalidMessage
	}
	if err := n.raw.Step(&decoded); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return nil
}

// ProcessReady durably records, applies, and advances one synchronous RawNode Ready.
func (n *Node) ProcessReady() (BatchDTO, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return BatchDTO{}, err
	}
	if !n.raw.HasReady() {
		return BatchDTO{}, nil
	}
	ready := n.raw.Ready()
	if err := n.store.appendReady(ready); err != nil {
		return BatchDTO{}, n.fail(err)
	}
	if n.onReadyEvent != nil {
		n.onReadyEvent("persisted")
	}

	messages := make([]MessageDTO, 0, len(ready.Messages))
	for _, message := range ready.Messages {
		data, err := proto.Marshal(message)
		if err != nil {
			return BatchDTO{}, n.fail(fmt.Errorf("marshal outbound message: %w", err))
		}
		if len(data) > maxMessageSize {
			return BatchDTO{}, n.fail(fmt.Errorf("outbound message exceeds %d bytes", maxMessageSize))
		}
		messages = append(messages, MessageDTO{From: NodeID(message.GetFrom()), To: NodeID(message.GetTo()), Data: bytes.Clone(data)})
	}
	if n.onReadyEvent != nil {
		n.onReadyEvent("outbound")
	}

	if ready.Snapshot != nil && !etcdraft.IsEmptySnap(ready.Snapshot) {
		n.confState = cloneConfState(ready.Snapshot.GetMetadata().GetConfState())
		n.appliedCommits = cloneCommits(n.store.commits)
		n.appliedIndex = ready.Snapshot.GetMetadata().GetIndex()
	}
	commits := make([]CommitDTO, 0, len(ready.CommittedEntries))
	for _, entry := range ready.CommittedEntries {
		if entry.GetIndex() <= n.appliedIndex {
			continue
		}
		switch entry.GetType() {
		case pb.EntryNormal:
			if len(entry.GetData()) != 0 {
				if len(entry.GetData()) > maxCommandSize {
					return BatchDTO{}, n.fail(fmt.Errorf("apply command at index %d: %w", entry.GetIndex(), ErrProposalTooLarge))
				}
				commit := CommitDTO{Index: entry.GetIndex(), Data: bytes.Clone(entry.GetData())}
				n.appliedCommits = append(n.appliedCommits, commit)
				commits = append(commits, CommitDTO{Index: entry.GetIndex(), Data: bytes.Clone(commit.Data)})
			}
		case pb.EntryConfChange, pb.EntryConfChangeV2:
			change, err := decodeConfChange(entry)
			if err != nil {
				return BatchDTO{}, n.fail(fmt.Errorf("apply configuration at index %d: %w", entry.GetIndex(), err))
			}
			n.confState, err = applyConfState(n.confState, change)
			if err != nil {
				return BatchDTO{}, n.fail(fmt.Errorf("apply configuration at index %d: %w", entry.GetIndex(), err))
			}
		default:
			return BatchDTO{}, n.fail(fmt.Errorf("apply unknown entry type %d", entry.GetType()))
		}
		n.appliedIndex = entry.GetIndex()
	}
	n.store.appliedIndex = n.appliedIndex
	n.store.confState = cloneConfState(n.confState)
	n.store.commits = cloneCommits(n.appliedCommits)
	if n.onReadyEvent != nil {
		n.onReadyEvent("applied")
	}
	if !n.bootstrapped && fixedConfState(n.confState, n.peerIDs) {
		if err := n.store.createSnapshot(n.appliedIndex, n.confState, n.appliedCommits); err != nil {
			return BatchDTO{}, n.fail(err)
		}
		n.bootstrapped = true
	}

	n.raw.Advance(ready)
	if n.onReadyEvent != nil {
		n.onReadyEvent("advanced")
	}
	return BatchDTO{Messages: messages, Commits: commits}, nil
}

// Snapshot creates and persists an immutable application snapshot at AppliedIndex.
func (n *Node) Snapshot() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if n.appliedIndex == 0 {
		return nil
	}
	if err := n.store.createSnapshot(n.appliedIndex, n.confState, n.appliedCommits); err != nil {
		return n.fail(err)
	}
	if fixedConfState(n.confState, n.peerIDs) {
		n.bootstrapped = true
	}
	return nil
}

// Status returns a deep, adapter-owned view of current Raft progress.
func (n *Node) Status() StatusDTO {
	n.mu.Lock()
	defer n.mu.Unlock()
	status := n.raw.Status()
	role := RoleFollower
	switch status.RaftState {
	case etcdraft.StateCandidate, etcdraft.StatePreCandidate:
		role = RoleCandidate
	case etcdraft.StateLeader:
		role = RoleLeader
	}
	term := uint64(0)
	commitIndex := uint64(0)
	if status.HardState != nil {
		term = status.HardState.GetTerm()
		commitIndex = status.HardState.GetCommit()
	}
	return StatusDTO{
		NodeID:       n.id,
		LeaderID:     NodeID(status.Lead),
		Role:         role,
		Term:         term,
		CommitIndex:  commitIndex,
		AppliedIndex: n.appliedIndex,
		Bootstrapped: n.bootstrapped,
	}
}

// AppliedCommits returns deep copies of all non-empty committed application records.
func (n *Node) AppliedCommits() []CommitDTO {
	n.mu.Lock()
	defer n.mu.Unlock()
	return cloneCommits(n.appliedCommits)
}

// Close closes the local WAL and makes all subsequent operations return ErrClosed.
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	if err := n.store.close(); err != nil {
		return err
	}
	return nil
}

func (n *Node) stateError() error {
	if n.closed {
		return ErrClosed
	}
	if n.failure != nil {
		return n.failure
	}
	return nil
}

func (n *Node) fail(err error) error {
	if n.failure == nil {
		n.failure = fmt.Errorf("%w: %w", ErrFailed, err)
	}
	return n.failure
}

func normalizePeerIDs(nodeID NodeID, peerIDs []NodeID) ([]NodeID, error) {
	if nodeID == 0 || len(peerIDs) != 3 {
		return nil, fmt.Errorf("%w: exactly three nonzero peer IDs including node are required", ErrConfigMismatch)
	}
	normalized := slices.Clone(peerIDs)
	sort.Slice(normalized, func(left, right int) bool { return normalized[left] < normalized[right] })
	for index, peerID := range normalized {
		if peerID == 0 || (index > 0 && normalized[index-1] == peerID) {
			return nil, fmt.Errorf("%w: peer IDs must be unique and nonzero", ErrConfigMismatch)
		}
	}
	if !slices.Contains(normalized, nodeID) {
		return nil, fmt.Errorf("%w: node ID is not a voter", ErrConfigMismatch)
	}
	return normalized, nil
}

func fixedConfState(state *pb.ConfState, peerIDs []NodeID) bool {
	if state == nil || len(state.Voters) != len(peerIDs) || len(state.Learners) != 0 || len(state.VotersOutgoing) != 0 || len(state.LearnersNext) != 0 || state.GetAutoLeave() {
		return false
	}
	voters := make([]NodeID, len(state.Voters))
	for index, voter := range state.Voters {
		voters[index] = NodeID(voter)
	}
	sort.Slice(voters, func(left, right int) bool { return voters[left] < voters[right] })
	return slices.Equal(voters, peerIDs)
}
