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
	"time"
	"unicode/utf8"

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
	maxDynamicCommandSize   = 1 * 1024 * 1024
	maxReadIndexContextSize = 128
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
	// ErrStorage means a durable WAL, snapshot, or metadata write failed.
	ErrStorage = errors.New("raft storage operation failed")
	// ErrNotLeader means a proposal or campaign operation requires a leader.
	ErrNotLeader = errors.New("raft node is not leader")
	// ErrEmptyCommand means a proposal has no application bytes.
	ErrEmptyCommand = errors.New("raft proposal is empty")
	// ErrProposalTooLarge means a proposal exceeds the fixed spike limit.
	ErrProposalTooLarge = errors.New("raft proposal is too large")
	// ErrBackpressure means proposal or local storage admission has no capacity.
	ErrBackpressure = errors.New("raft proposal backpressure")
	// ErrInvalidMessage means a peer message failed the adapter boundary checks.
	ErrInvalidMessage = errors.New("invalid raft peer message")
	// ErrCorruptWAL means a WAL cannot be safely replayed.
	ErrCorruptWAL = errors.New("corrupt raft WAL")
	// ErrCorruptSnapshot means a finalized snapshot cannot be safely restored.
	ErrCorruptSnapshot = errors.New("corrupt raft snapshot")
	// ErrMembershipChangeInProgress means a prior ConfChangeV2 is not yet committed.
	ErrMembershipChangeInProgress = errors.New("raft membership change is in progress")
	// ErrInvalidLeadershipTransfer means leadership cannot be transferred to the requested member.
	ErrInvalidLeadershipTransfer = errors.New("invalid raft leadership transfer")
	// ErrApplySequence means the consumer did not acknowledge its current batch.
	ErrApplySequence = errors.New("invalid raft application completion sequence")
)

var discardRaftLogger = &etcdraft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

// NodeID is a persistent, never-reused Raft member identity.
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

// Config selects the fixed peer-only profile or the dynamic cluster profile.
type Config struct {
	NodeID            NodeID
	PeerIDs           []NodeID
	Directory         string
	DynamicMembership bool
	ClusterID         string
	CertificateSHA256 [32]byte
	BootstrapVoterIDs []NodeID
	// PersistenceBoundaryHook observes storage boundaries. WAL hooks run on the
	// single WAL worker; local snapshot hooks run on the caller. Hooks must not
	// call Node or retain borrowed payloads.
	PersistenceBoundaryHook func(PersistenceBoundaryDTO)
	// WALBoundaryHook observes entry frames on the single WAL worker.
	WALBoundaryHook func(WALBoundaryDTO)
	// WALSyncObserver observes one successful fsync, including all grouped frames.
	// It runs on the WAL worker and must not block or call Node.
	WALSyncObserver func(WALSyncDTO)
	// StorageActivityObserver brackets each worker I/O batch (true, then false).
	// It may run concurrently with health readers and must not call Node.
	StorageActivityObserver func(bool)
}

// WALBoundaryStage identifies one step of appending a Ready frame to wal.bin.
type WALBoundaryStage uint8

const (
	// WALBeforeWrite runs before the frame header is written.
	WALBeforeWrite WALBoundaryStage = iota + 1
	// WALBeforeSync runs after the frame header and payload writes, before File.Sync.
	WALBeforeSync
	// WALAfterSync runs after File.Sync succeeds, before snapshot and memory updates.
	WALAfterSync
)

// WALBoundaryDTO describes a Ready WAL frame carrying non-empty normal entries.
type WALBoundaryDTO struct {
	Stage   WALBoundaryStage
	Entries [][]byte // borrowed entry payloads in log order; valid only during the call
}

// WALSyncDTO describes one successful fsync covering one or more WAL frames.
type WALSyncDTO struct {
	Duration   time.Duration
	Entries    int
	FrameBytes int
}

// MembershipChangeAction identifies one supported single-step ConfChangeV2.
type MembershipChangeAction uint8

const (
	// MembershipAddLearner adds a new non-voting member.
	MembershipAddLearner MembershipChangeAction = iota + 1
	// MembershipPromoteVoter promotes a caught-up learner to voter.
	MembershipPromoteVoter
	// MembershipRemove removes a voter or learner after planned transfer.
	MembershipRemove
)

// PersistenceStage identifies one side of stable Ready or snapshot persistence.
type PersistenceStage uint8

const (
	// PersistenceBeforeReady identifies the phase immediately before appending Ready to stable storage.
	PersistenceBeforeReady PersistenceStage = iota + 1
	// PersistenceAfterReady identifies the phase immediately after appending Ready to stable storage.
	PersistenceAfterReady
	// PersistenceBeforeSnapshotFinalize identifies the phase after snapshot temp-file sync and before rename.
	PersistenceBeforeSnapshotFinalize
	// PersistenceAfterSnapshotFinalize identifies the phase after snapshot rename and directory sync.
	PersistenceAfterSnapshotFinalize
)

// PersistenceBoundaryDTO describes membership-relevant Ready or local snapshot persistence.
type PersistenceBoundaryDTO struct {
	Stage                PersistenceStage
	ConfigurationActions []MembershipChangeAction // dynamic ConfChangeV2 entries in Ready.Entries, log order
	InstallsSnapshot     bool                     // Ready carries a non-empty incoming snapshot
	CreatesSnapshot      bool                     // local typed snapshot file finalization
}

// MembershipChangeDTO describes one committed Raft voter-set transition request.
type MembershipChangeDTO struct {
	OperationID string
	MemberID    NodeID
	Action      MembershipChangeAction
}

// CommittedConfigurationDTO identifies an applied dynamic membership transition.
type CommittedConfigurationDTO struct {
	Index       uint64
	OperationID string
	MemberID    NodeID
	Action      MembershipChangeAction
}

// ReadStateDTO is an asynchronous quorum ReadIndex completion.
type ReadStateDTO struct {
	RequestContext []byte
	Index          uint64
}

// MessageDTO is the adapter-owned serialized peer message.
type MessageDTO struct {
	From       NodeID
	To         NodeID
	Data       []byte
	IsSnapshot bool
	Type       string
}

// StepResultDTO classifies accepted work without exposing Raft protobuf types.
type StepResultDTO struct {
	Proposal         bool
	ReadIndexRequest bool
}

// BatchDTO contains outbound messages, committed commands, snapshots, and read barriers.
type BatchDTO struct {
	Messages             []MessageDTO
	Commands             [][]byte
	CommandIndexes       []uint64
	ConfigurationChanges []CommittedConfigurationDTO
	SnapshotData         []byte
	SnapshotIndex        uint64
	ReadStates           []ReadStateDTO
	// Progress reports local Ready work even when there is no consumer output.
	Progress bool
	// ApplySequence must be completed after the consumer applies this batch.
	ApplySequence uint64
}

// AppliedCommandDTO pairs one committed application command with its Raft log index.
type AppliedCommandDTO struct {
	Index uint64
	Data  []byte
}

// StatusDTO reports the node's deterministic Raft and committed membership progress.
type StatusDTO struct {
	NodeID            NodeID
	LeaderID          NodeID
	Role              Role
	Term              uint64
	CommitIndex       uint64
	AppliedIndex      uint64
	Bootstrapped      bool
	VoterIDs          []NodeID
	LearnerIDs        []NodeID
	DynamicMembership bool
}

// Node serializes RawNode and memory state while one owned WAL worker persists
// immutable append requests. Callers own logical ticks and application apply.
type Node struct {
	mu sync.Mutex

	id      NodeID
	peerIDs []NodeID
	dynamic bool
	store   *stableStore
	raw     *etcdraft.RawNode
	closed  bool
	failure error

	writer               *WALAppendWorker
	appendPending        int
	appendBytes          int
	appendSequence       uint64
	durableSequence      uint64
	heldAppend           *walAppendRequest
	applyQueue           []storageApplyRequest
	applying             *storageApplyRequest
	applySequence        uint64
	readyInputBytes      int
	bootstrapped         bool
	confState            *pb.ConfState
	appliedIndex         uint64
	appliedCommands      [][]byte
	commandIndexes       []uint64
	configurationChanges []CommittedConfigurationDTO
	stateSnapshot        []byte
	confChangePending    bool
}

// Open opens a fixed-voter peer-only node or a v2 dynamic cluster member.
func Open(config Config) (*Node, error) {
	var (
		peerIDs         []NodeID
		initialVoterIDs []NodeID
		isNew           bool
		store           *stableStore
		err             error
	)
	if config.DynamicMembership {
		if len(config.PeerIDs) != 0 || !validClusterID(config.ClusterID) || config.CertificateSHA256 == ([32]byte{}) {
			return nil, ErrConfigMismatch
		}
		initialVoterIDs, err = normalizeBootstrapVoterIDs(config.NodeID, config.BootstrapVoterIDs)
		if err != nil {
			return nil, err
		}
		isNew, err = prepareDynamicClusterDirectory(config.Directory, config.ClusterID, config.NodeID, config.CertificateSHA256, initialVoterIDs)
		if err != nil {
			return nil, err
		}
		store, err = openDynamicStableStore(config.Directory)
		if err != nil {
			return nil, err
		}
		store.walHook = config.WALBoundaryHook
		store.walSyncObserver = config.WALSyncObserver
		if config.PersistenceBoundaryHook != nil {
			store.finalizeHook = func(stage PersistenceStage) {
				config.PersistenceBoundaryHook(PersistenceBoundaryDTO{Stage: stage, CreatesSnapshot: true})
			}
		}
		if !isNew && len(initialVoterIDs) != 0 {
			lastIndex, indexErr := store.memory.LastIndex()
			if indexErr != nil {
				_ = store.close()
				return nil, fmt.Errorf("read bootstrap WAL index: %w", indexErr)
			}
			if lastIndex == 0 {
				_ = store.close()
				return nil, ErrBootstrapIncomplete
			}
		}
	} else {
		if config.ClusterID != "" || config.CertificateSHA256 != ([32]byte{}) || len(config.BootstrapVoterIDs) != 0 {
			return nil, ErrConfigMismatch
		}
		peerIDs, err = normalizePeerIDs(config.NodeID, config.PeerIDs)
		if err != nil {
			return nil, err
		}
		isNew, err = prepareClusterDirectory(config.Directory, config.NodeID, peerIDs)
		if err != nil {
			return nil, err
		}
		store, err = openStableStore(config.Directory)
		if err != nil {
			return nil, err
		}
		store.walHook = config.WALBoundaryHook
		store.walSyncObserver = config.WALSyncObserver
	}

	raftConfig := &etcdraft.Config{
		ID:                        uint64(config.NodeID),
		ElectionTick:              electionTick,
		HeartbeatTick:             heartbeatTick,
		Storage:                   store.memory,
		Applied:                   store.appliedIndex,
		AsyncStorageWrites:        true,
		MaxSizePerMsg:             maxSizePerMessage,
		MaxCommittedSizePerReady:  maxCommittedReadySize,
		MaxUncommittedEntriesSize: maxUncommittedEntrySize,
		MaxInflightMsgs:           maxInflightMessages,
		MaxInflightBytes:          maxInflightBytes,
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: false,
		Logger:                    discardRaftLogger,
	}
	raw, err := etcdraft.NewRawNode(raftConfig)
	if err != nil {
		_ = store.close()
		return nil, fmt.Errorf("create raft node: %w", err)
	}
	if isNew && !config.DynamicMembership || isNew && len(initialVoterIDs) != 0 {
		bootstrapIDs := peerIDs
		if config.DynamicMembership {
			bootstrapIDs = initialVoterIDs
		}
		peers := make([]etcdraft.Peer, len(bootstrapIDs))
		for index, peerID := range bootstrapIDs {
			peers[index] = etcdraft.Peer{ID: uint64(peerID)}
		}
		if err := raw.Bootstrap(peers); err != nil {
			_ = store.close()
			return nil, fmt.Errorf("bootstrap raft node: %w", err)
		}
	}

	_, initialConfState, err := store.memory.InitialState()
	if err != nil {
		_ = store.close()
		return nil, fmt.Errorf("read initial raft configuration: %w", err)
	}
	if isNew && (!config.DynamicMembership || len(initialVoterIDs) != 0) {
		bootstrapIDs := peerIDs
		if config.DynamicMembership {
			bootstrapIDs = initialVoterIDs
		}
		initialConfState = &pb.ConfState{Voters: make([]uint64, len(bootstrapIDs))}
		for index, voterID := range bootstrapIDs {
			initialConfState.Voters[index] = uint64(voterID)
		}
	}
	node := &Node{
		id:                   config.NodeID,
		peerIDs:              slices.Clone(peerIDs),
		dynamic:              config.DynamicMembership,
		store:                store,
		raw:                  raw,
		confState:            cloneConfState(initialConfState),
		appliedIndex:         store.appliedIndex,
		appliedCommands:      cloneCommands(store.commands),
		commandIndexes:       slices.Clone(store.commandIndexes),
		configurationChanges: slices.Clone(store.configurationChanges),
		stateSnapshot:        bytes.Clone(store.snapshotData),
	}
	for _, change := range store.confChanges {
		node.confState = cloneConfState(raw.ApplyConfChange(change))
	}
	if !isNew && !confStateEqual(node.confState, store.confState) {
		_ = store.close()
		return nil, fmt.Errorf("%w: recovered ConfState differs from committed configuration entries", ErrConfigMismatch)
	}
	node.bootstrapped = fixedConfState(store.confState, peerIDs)
	if config.DynamicMembership {
		node.bootstrapped = containsMember(store.confState, config.NodeID)
	}
	if config.DynamicMembership {
		node.confChangePending = node.hasUncommittedConfChange()
	}
	node.writer = newWALAppendWorker(store, config.PersistenceBoundaryHook, config.StorageActivityObserver)
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

// TransferLeadership asks the current leader to transfer to an up-to-date voter.
func (n *Node) TransferLeadership(target NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if !n.dynamic || target == 0 || target == n.id || !slices.Contains(n.confState.GetVoters(), uint64(target)) {
		return ErrInvalidLeadershipTransfer
	}
	if n.raw.Status().RaftState != etcdraft.StateLeader {
		return ErrNotLeader
	}
	n.raw.TransferLeader(uint64(target))
	return nil
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

// Propose submits a copied application command only while this node is leader.
func (n *Node) Propose(data []byte) error {
	return n.propose(data, false)
}

// ProposeForwarded submits a copied command locally and lets Raft forward it when this node is a follower.
func (n *Node) ProposeForwarded(data []byte) error {
	return n.propose(data, true)
}

// ProposeBatchForwarded submits ordered independent entries in one MsgProp.
// Validation is atomic; storage, commitment and per-entry apply remain unchanged.
func (n *Node) ProposeBatchForwarded(commands [][]byte) error {
	if len(commands) == 1 {
		return n.propose(commands[0], true)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	status := n.raw.BasicStatus()
	if status.RaftState != etcdraft.StateLeader && status.Lead == etcdraft.None {
		return ErrNotLeader
	}
	if len(commands) == 0 {
		return ErrEmptyCommand
	}
	if len(commands) > 64 {
		return ErrProposalTooLarge
	}
	limit := maxProposalSize
	if n.dynamic {
		limit = maxDynamicCommandSize
	}
	entries := make([]*pb.Entry, len(commands))
	for index, command := range commands {
		if len(command) == 0 {
			return ErrEmptyCommand
		}
		if len(command) > limit {
			return ErrProposalTooLarge
		}
		entries[index] = &pb.Entry{Data: command}
	}
	message := &pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(n.id)), Entries: entries}
	// A follower forwards this message intact; bound its encoded wire size too.
	if proto.Size(message)+32 > maxMessageSize {
		return ErrProposalTooLarge
	}
	if n.storageSaturated() || n.readyInputBytes+proto.Size(message) > maxWALFramePayload-1024 {
		return ErrBackpressure
	}
	for _, entry := range entries {
		entry.Data = bytes.Clone(entry.Data)
	}
	if err := n.raw.Step(message); err != nil {
		if errors.Is(err, etcdraft.ErrProposalDropped) {
			return fmt.Errorf("%w: %v", ErrBackpressure, err)
		}
		return err
	}
	n.readyInputBytes += proto.Size(message)
	return nil
}

func (n *Node) propose(data []byte, forwardToLeader bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	status := n.raw.BasicStatus()
	if status.RaftState != etcdraft.StateLeader && (!forwardToLeader || status.Lead == etcdraft.None) {
		return ErrNotLeader
	}
	if len(data) == 0 {
		return ErrEmptyCommand
	}
	limit := maxProposalSize
	if n.dynamic {
		limit = maxDynamicCommandSize
	}
	if len(data) > limit {
		return ErrProposalTooLarge
	}
	if n.storageSaturated() || n.readyInputBytes+len(data)+32 > maxWALFramePayload-1024 {
		return ErrBackpressure
	}
	command := bytes.Clone(data)
	if err := n.raw.Propose(command); err != nil {
		if errors.Is(err, etcdraft.ErrProposalDropped) {
			return fmt.Errorf("%w: %v", ErrBackpressure, err)
		}
		return err
	}
	n.readyInputBytes += len(data) + 32
	return nil
}

// ProposeMembershipChange queues one single-step ConfChangeV2 on the current leader.
func (n *Node) ProposeMembershipChange(change MembershipChangeDTO) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if !n.dynamic || change.MemberID == 0 || etcdraft.IsLocalMsgTarget(uint64(change.MemberID)) || !validOperationID(change.OperationID) {
		return ErrConfigMismatch
	}
	if n.raw.Status().RaftState != etcdraft.StateLeader {
		return ErrNotLeader
	}
	if n.confChangePending || n.hasUncommittedConfChange() {
		return ErrMembershipChangeInProgress
	}
	if n.storageSaturated() {
		return ErrBackpressure
	}
	var changeType pb.ConfChangeType
	switch change.Action {
	case MembershipAddLearner:
		if containsMember(n.confState, change.MemberID) {
			return ErrConfigMismatch
		}
		changeType = pb.ConfChangeAddLearnerNode
	case MembershipPromoteVoter:
		if !slices.Contains(n.confState.GetLearners(), uint64(change.MemberID)) {
			return ErrConfigMismatch
		}
		changeType = pb.ConfChangeAddNode
	case MembershipRemove:
		if !containsMember(n.confState, change.MemberID) {
			return ErrConfigMismatch
		}
		changeType = pb.ConfChangeRemoveNode
	default:
		return ErrConfigMismatch
	}
	confChange := &pb.ConfChangeV2{
		Changes: []*pb.ConfChangeSingle{{Type: changeType.Enum(), NodeId: new(uint64(change.MemberID))}},
		Context: []byte(change.OperationID),
	}
	if err := n.raw.ProposeConfChange(confChange); err != nil {
		if errors.Is(err, etcdraft.ErrProposalDropped) {
			return fmt.Errorf("%w: %v", ErrBackpressure, err)
		}
		return err
	}
	n.confChangePending = true
	return nil
}

// ReadIndex requests an asynchronous fresh quorum barrier through the local Raft node.
func (n *Node) ReadIndex(requestContext []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if len(requestContext) == 0 || len(requestContext) > maxReadIndexContextSize {
		return ErrInvalidMessage
	}
	n.raw.ReadIndex(bytes.Clone(requestContext))
	return nil
}

// ApplicationSnapshot returns a copy of the latest typed-state snapshot payload.
func (n *Node) ApplicationSnapshot() []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return bytes.Clone(n.stateSnapshot)
}

// SnapshotWithApplicationData compacts a dynamic cluster at its current applied index.
func (n *Node) SnapshotWithApplicationData(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if !n.dynamic || len(data) == 0 || len(data) > maxSnapshotPayload {
		return ErrConfigMismatch
	}
	if n.appendPending != 0 || n.heldAppend != nil || n.applying != nil || len(n.applyQueue) != 0 {
		return ErrBackpressure
	}
	if n.appliedIndex == 0 {
		return nil
	}
	if err := n.store.createSnapshotData(n.appliedIndex, n.confState, data); err != nil {
		return n.fail(fmt.Errorf("%w: %w", ErrStorage, err))
	}
	n.appliedCommands = nil
	n.commandIndexes = nil
	n.configurationChanges = nil
	n.store.commands = nil
	n.store.commandIndexes = nil
	n.store.configurationChanges = nil
	return nil
}

// Step validates and delivers one serialized peer message to the local RawNode.
// Its result allows the driver to flush read fan-out without splitting replication batches.
func (n *Node) Step(message MessageDTO) (StepResultDTO, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return StepResultDTO{}, err
	}
	if message.From == 0 || message.To == 0 || message.To != n.id || len(message.Data) == 0 || len(message.Data) > maxMessageSize {
		return StepResultDTO{}, ErrInvalidMessage
	}
	payload := bytes.Clone(message.Data)
	var decoded pb.Message
	if err := proto.Unmarshal(payload, &decoded); err != nil {
		return StepResultDTO{}, fmt.Errorf("%w: decode message: %v", ErrInvalidMessage, err)
	}
	if decoded.GetTo() != uint64(message.To) || decoded.GetFrom() == 0 || decoded.GetTo() == 0 {
		return StepResultDTO{}, ErrInvalidMessage
	}
	if decoded.GetFrom() != uint64(message.From) {
		// etcd/raft forwards these requests without replacing the original
		// requester. Only an authenticated current member may relay one, and
		// the logical requester must also remain in the committed ConfState.
		if !n.dynamic || !containsMember(n.confState, message.From) ||
			!containsMember(n.confState, NodeID(decoded.GetFrom())) ||
			(decoded.GetType() != pb.MsgProp && decoded.GetType() != pb.MsgReadIndex &&
				decoded.GetType() != pb.MsgTransferLeader) {
			return StepResultDTO{}, ErrInvalidMessage
		}
	}
	if etcdraft.IsLocalMsgTarget(decoded.GetFrom()) || etcdraft.IsLocalMsgTarget(decoded.GetTo()) ||
		decoded.GetType() == pb.MsgStorageAppend || decoded.GetType() == pb.MsgStorageAppendResp ||
		decoded.GetType() == pb.MsgStorageApply || decoded.GetType() == pb.MsgStorageApplyResp {
		return StepResultDTO{}, ErrInvalidMessage
	}
	if decoded.GetType() == pb.MsgProp || decoded.GetType() == pb.MsgApp || decoded.GetType() == pb.MsgSnap {
		if n.storageSaturated() || (decoded.GetType() != pb.MsgSnap &&
			n.readyInputBytes+len(message.Data) > maxWALFramePayload-1024) {
			return StepResultDTO{}, ErrBackpressure
		}
		n.readyInputBytes += len(message.Data)
	}
	if err := n.raw.Step(&decoded); err != nil {
		return StepResultDTO{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	proposal := decoded.GetType() == pb.MsgProp && n.raw.BasicStatus().RaftState == etcdraft.StateLeader
	return StepResultDTO{Proposal: proposal, ReadIndexRequest: decoded.GetType() == pb.MsgReadIndex}, nil
}

// ReportPeerMessageFailure tells RawNode a transport rejected an outbound message.
// Snapshot failure is reported separately so Raft can leave snapshot state and retry.
func (n *Node) ReportPeerMessageFailure(peerID NodeID, isSnapshot bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	knownPeer := slices.Contains(n.peerIDs, peerID)
	if n.dynamic {
		knownPeer = containsMember(n.confState, peerID)
	}
	if peerID == 0 || peerID == n.id || !knownPeer {
		return ErrInvalidMessage
	}
	if isSnapshot {
		n.raw.ReportSnapshot(uint64(peerID), etcdraft.SnapshotFailure)
	}
	n.raw.ReportUnreachable(uint64(peerID))
	return nil
}

// ProcessReady polls storage completions and accepts bounded asynchronous Ready
// work. It never waits for WAL I/O. CompleteApply acknowledges consumer output.
func (n *Node) ProcessReady() (BatchDTO, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return BatchDTO{}, err
	}
	return n.processReadyLocked()
}

// Snapshot creates and persists an immutable application snapshot at AppliedIndex.
func (n *Node) Snapshot() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if n.dynamic {
		return ErrConfigMismatch
	}
	if n.appendPending != 0 || n.heldAppend != nil || n.applying != nil || len(n.applyQueue) != 0 {
		return ErrBackpressure
	}
	if n.appliedIndex == 0 {
		return nil
	}
	if err := n.store.createSnapshot(n.appliedIndex, n.confState, n.appliedCommands); err != nil {
		return n.fail(fmt.Errorf("%w: %w", ErrStorage, err))
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
	status := n.raw.BasicStatus()
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
	var voters, learners []NodeID
	if n.dynamic {
		voters = nodeIDs(n.confState.GetVoters())
		learners = nodeIDs(n.confState.GetLearners())
	}
	return StatusDTO{
		NodeID:            n.id,
		LeaderID:          NodeID(status.Lead),
		Role:              role,
		Term:              term,
		CommitIndex:       commitIndex,
		AppliedIndex:      n.appliedIndex,
		Bootstrapped:      n.bootstrapped,
		VoterIDs:          voters,
		LearnerIDs:        learners,
		DynamicMembership: n.dynamic,
	}
}

// AppliedCommands returns deep copies of all non-empty committed application commands.
func (n *Node) AppliedCommands() [][]byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return cloneCommands(n.appliedCommands)
}

// AppliedCommandRecords returns indexed copies of commands since the latest snapshot.
func (n *Node) AppliedCommandRecords() []AppliedCommandDTO {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.dynamic || len(n.appliedCommands) != len(n.commandIndexes) {
		return nil
	}
	records := make([]AppliedCommandDTO, len(n.appliedCommands))
	for index, command := range n.appliedCommands {
		records[index] = AppliedCommandDTO{Index: n.commandIndexes[index], Data: bytes.Clone(command)}
	}
	return records
}

// AppliedConfigurationChanges returns committed dynamic configuration events since the latest snapshot.
func (n *Node) AppliedConfigurationChanges() []CommittedConfigurationDTO {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.dynamic {
		return nil
	}
	return slices.Clone(n.configurationChanges)
}

// PeerMatchIndex reports the last log index matched by one current dynamic member.
func (n *Node) PeerMatchIndex(peerID NodeID) (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil || !n.dynamic || peerID == 0 || peerID == n.id || !containsMember(n.confState, peerID) {
		return 0, false
	}
	progress, ok := n.raw.Status().Progress[uint64(peerID)]
	if !ok {
		return 0, false
	}
	return progress.Match, true
}

// Close closes the local WAL and makes all subsequent operations return ErrClosed.
func (n *Node) Close() error {
	n.mu.Lock()
	if !n.closed {
		n.closed = true
		n.writer.finalAppend = n.heldAppend
		close(n.writer.requests)
	}
	n.mu.Unlock()
	<-n.writer.done
	n.mu.Lock()
	defer n.mu.Unlock()
	var finalError error
	if n.heldAppend != nil || len(n.writer.requests) != 0 {
		finalError = n.writer.finalError
		if finalError == nil && n.failure != nil {
			finalError = n.failure
		}
		n.heldAppend = nil
		// A terminal worker may have exited before Close published finalAppend.
		for len(n.writer.results) != 0 {
			result := <-n.writer.results
			if result.err != nil {
				finalError = result.err
			}
		}
		for len(n.writer.requests) != 0 {
			<-n.writer.requests // Terminal failure was reported; release dead work.
		}
		if finalError != nil {
			finalError = fmt.Errorf("%w: pending append on close: %w", ErrStorage, finalError)
		}
	}
	return errors.Join(finalError, n.store.close())
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
	if nodeID == 0 || etcdraft.IsLocalMsgTarget(uint64(nodeID)) || len(peerIDs) != 3 {
		return nil, fmt.Errorf("%w: exactly three nonzero peer IDs including node are required", ErrConfigMismatch)
	}
	normalized := slices.Clone(peerIDs)
	sort.Slice(normalized, func(left, right int) bool { return normalized[left] < normalized[right] })
	for index, peerID := range normalized {
		if peerID == 0 || etcdraft.IsLocalMsgTarget(uint64(peerID)) || (index > 0 && normalized[index-1] == peerID) {
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

func normalizeBootstrapVoterIDs(nodeID NodeID, voterIDs []NodeID) ([]NodeID, error) {
	if nodeID == 0 || etcdraft.IsLocalMsgTarget(uint64(nodeID)) {
		return nil, ErrConfigMismatch
	}
	if len(voterIDs) == 0 {
		return nil, nil
	}
	if len(voterIDs) != 3 || !slices.IsSorted(voterIDs) || !slices.Contains(voterIDs, nodeID) {
		return nil, fmt.Errorf("%w: bootstrap requires three sorted voter IDs including the local member", ErrConfigMismatch)
	}
	for index, voterID := range voterIDs {
		if voterID == 0 || etcdraft.IsLocalMsgTarget(uint64(voterID)) || (index > 0 && voterIDs[index-1] == voterID) {
			return nil, fmt.Errorf("%w: bootstrap voter IDs must be unique and nonzero", ErrConfigMismatch)
		}
	}
	return slices.Clone(voterIDs), nil
}

func validClusterID(clusterID string) bool {
	return len(clusterID) >= 1 && len(clusterID) <= 200 && utf8.ValidString(clusterID)
}

func validOperationID(operationID string) bool {
	if len(operationID) != 36 || operationID[8] != '-' || operationID[13] != '-' || operationID[18] != '-' || operationID[23] != '-' {
		return false
	}
	for index, value := range operationID {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return false
		}
	}
	return true
}

func containsMember(state *pb.ConfState, memberID NodeID) bool {
	if state == nil || memberID == 0 {
		return false
	}
	id := uint64(memberID)
	return slices.Contains(state.GetVoters(), id) ||
		slices.Contains(state.GetLearners(), id) ||
		slices.Contains(state.GetVotersOutgoing(), id) ||
		slices.Contains(state.GetLearnersNext(), id)
}

func nodeIDs(ids []uint64) []NodeID {
	result := make([]NodeID, len(ids))
	for index, id := range ids {
		result[index] = NodeID(id)
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}

func confStateEqual(left, right *pb.ConfState) bool {
	leftCopy := cloneConfState(left)
	rightCopy := cloneConfState(right)
	sort.Slice(leftCopy.Voters, func(i, j int) bool { return leftCopy.Voters[i] < leftCopy.Voters[j] })
	sort.Slice(leftCopy.Learners, func(i, j int) bool { return leftCopy.Learners[i] < leftCopy.Learners[j] })
	sort.Slice(leftCopy.VotersOutgoing, func(i, j int) bool { return leftCopy.VotersOutgoing[i] < leftCopy.VotersOutgoing[j] })
	sort.Slice(leftCopy.LearnersNext, func(i, j int) bool { return leftCopy.LearnersNext[i] < leftCopy.LearnersNext[j] })
	sort.Slice(rightCopy.Voters, func(i, j int) bool { return rightCopy.Voters[i] < rightCopy.Voters[j] })
	sort.Slice(rightCopy.Learners, func(i, j int) bool { return rightCopy.Learners[i] < rightCopy.Learners[j] })
	sort.Slice(rightCopy.VotersOutgoing, func(i, j int) bool { return rightCopy.VotersOutgoing[i] < rightCopy.VotersOutgoing[j] })
	sort.Slice(rightCopy.LearnersNext, func(i, j int) bool { return rightCopy.LearnersNext[i] < rightCopy.LearnersNext[j] })
	if leftCopy.GetAutoLeave() != rightCopy.GetAutoLeave() {
		return false
	}
	leftCopy.AutoLeave = nil
	rightCopy.AutoLeave = nil
	return proto.Equal(leftCopy, rightCopy)
}

func validateDynamicConfStateTransition(current *pb.ConfState, action MembershipChangeAction, memberID NodeID) error {
	if current == nil || memberID == 0 || len(current.GetVotersOutgoing()) != 0 ||
		len(current.GetLearnersNext()) != 0 || current.GetAutoLeave() {
		return fmt.Errorf("configuration state is not a stable single-step membership state")
	}
	switch action {
	case MembershipAddLearner:
		if containsMember(current, memberID) {
			return fmt.Errorf("member %d is already present", memberID)
		}
	case MembershipPromoteVoter:
		if !slices.Contains(current.GetLearners(), uint64(memberID)) {
			return fmt.Errorf("member %d is not a learner", memberID)
		}
	case MembershipRemove:
		if !containsMember(current, memberID) {
			return fmt.Errorf("member %d is not present", memberID)
		}
	default:
		return fmt.Errorf("unsupported membership action %d", action)
	}
	return nil
}

func (n *Node) hasUncommittedConfChange() bool {
	commitIndex := n.store.hardState.GetCommit()
	if commitIndex == ^uint64(0) {
		return false
	}
	firstIndex, err := n.store.memory.FirstIndex()
	if err != nil {
		return true
	}
	low := commitIndex + 1
	if low < firstIndex {
		low = firstIndex
	}
	lastIndex, err := n.store.memory.LastIndex()
	if err != nil {
		return true
	}
	if low > lastIndex || lastIndex == ^uint64(0) {
		return false
	}
	entries, err := n.store.memory.Entries(low, lastIndex+1, ^uint64(0))
	if err != nil {
		return true
	}
	for _, entry := range entries {
		if entry.GetType() == pb.EntryConfChange || entry.GetType() == pb.EntryConfChangeV2 {
			return true
		}
	}
	return false
}
