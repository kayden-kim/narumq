package raft

import (
	"bytes"
	"fmt"
	"slices"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type storageApplyRequest struct {
	sequence       uint64
	requiredAppend uint64
	entries        []*pb.Entry
	responses      []*pb.Message
}

// StorageReady signals that polling can consume WAL completions. Notifications
// coalesce: ProcessReady must drain available work, not count notifications.
func (n *Node) StorageReady() <-chan struct{} { return n.writer.ready }

// StoragePending includes accepted work, consumer apply and unaccepted Ready.
// A completed ProcessReady call alone is not a durable/application checkpoint.
func (n *Node) StoragePending() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.appendPending != 0 || n.heldAppend != nil || len(n.applyQueue) != 0 || n.applying != nil || n.raw.HasReady()
}

func (n *Node) storageSaturated() bool {
	return n.heldAppend != nil || n.appendPending >= walAppendCapacity || n.appendBytes >= walAppendBytes
}

func (n *Node) enqueueAppend(request *walAppendRequest) bool {
	if n.appendPending >= walAppendCapacity || (n.appendPending != 0 && n.appendBytes+request.bytes > walAppendBytes) {
		return false
	}
	// An oversized snapshot gets the worker exclusively. Normal frames fit the
	// byte budget; a second request cannot follow it until completion is consumed.
	n.appendPending++
	n.appendBytes += request.bytes
	n.writer.requests <- request // Accounting guarantees a free buffered slot.
	return true
}

func (n *Node) processReadyLocked() (BatchDTO, error) {
	var batch BatchDTO
	if n.applying != nil {
		return batch, nil // The consumer must acknowledge the previous batch first.
	}
completions:
	for {
		select {
		case result := <-n.writer.results:
			if result.err != nil {
				return BatchDTO{}, n.fail(fmt.Errorf("%w: %w", ErrStorage, result.err))
			}
			request := result.request
			if request.sequence != n.durableSequence+1 {
				return BatchDTO{}, n.fail(fmt.Errorf("out-of-order WAL completion %d after %d", request.sequence, n.durableSequence))
			}
			if err := n.store.applyRecord(request.record); err != nil {
				return BatchDTO{}, n.fail(fmt.Errorf("%w: %w", ErrStorage, err))
			}
			n.appendPending--
			n.appendBytes -= request.bytes
			n.durableSequence = request.sequence
			batch.Progress = true
			if request.record.snapshot != nil {
				snapshot := request.record.snapshot
				n.confState = cloneConfState(snapshot.GetMetadata().GetConfState())
				n.appliedCommands = n.store.commands
				n.commandIndexes = n.store.commandIndexes
				n.configurationChanges = n.store.configurationChanges
				n.appliedIndex = snapshot.GetMetadata().GetIndex()
				n.bootstrapped = containsMember(n.confState, n.id)
				if n.dynamic {
					n.stateSnapshot = n.store.snapshotData
					batch.SnapshotData = bytes.Clone(snapshot.GetData())
					batch.SnapshotIndex = n.appliedIndex
				}
			}
			batch.Messages = append(batch.Messages, request.messages...)
			if err := n.deliverStorageResponses(request.responses, &batch); err != nil {
				return BatchDTO{}, n.fail(err)
			}
			if request.record.snapshot != nil {
				// Restore the consumer before exposing any subsequent apply work.
				n.applySequence++
				n.applying = &storageApplyRequest{sequence: n.applySequence, requiredAppend: request.sequence}
				batch.ApplySequence = n.applySequence
				return batch, nil
			}
		default:
			break completions
		}
	}
	if n.heldAppend != nil && n.enqueueAppend(n.heldAppend) {
		n.heldAppend = nil
		batch.Progress = true
	}
	if n.heldAppend == nil && !n.storageSaturated() && len(n.applyQueue) < walAppendCapacity && n.raw.HasReady() {
		ready := n.raw.Ready()
		n.readyInputBytes = 0
		batch.Progress = true
		for _, state := range ready.ReadStates {
			batch.ReadStates = append(batch.ReadStates, ReadStateDTO{RequestContext: bytes.Clone(state.RequestCtx), Index: state.Index})
		}
		var appendRequest *walAppendRequest
		for _, message := range ready.Messages {
			if message.GetType() == pb.MsgStorageAppend {
				request, err := n.newAppendRequest(message)
				if err != nil {
					return BatchDTO{}, n.fail(fmt.Errorf("%w: %w", ErrStorage, err))
				}
				appendRequest = request
				break
			}
		}
		for _, message := range ready.Messages {
			switch message.GetType() {
			case pb.MsgStorageAppend:
				// Enqueue after collecting replication dependencies below.
			case pb.MsgStorageApply:
				n.applySequence++
				n.applyQueue = append(n.applyQueue, storageApplyRequest{
					sequence: n.applySequence, requiredAppend: n.appendSequence,
					entries: slices.Clone(message.Entries), responses: slices.Clone(message.Responses),
				})
			default:
				if message.GetType() == pb.MsgApp && len(message.Entries) != 0 && (appendRequest != nil || n.appendPending != 0) {
					if appendRequest == nil {
						request, err := n.newAppendRequest(&pb.Message{})
						if err != nil {
							return BatchDTO{}, n.fail(fmt.Errorf("%w: %w", ErrStorage, err))
						}
						appendRequest = request
					}
					encoded, err := encodePeerMessage(message)
					if err != nil {
						return BatchDTO{}, n.fail(err)
					}
					appendRequest.messages = append(appendRequest.messages, encoded)
					appendRequest.bytes += len(encoded.Data) + 8
				} else if err := appendPeerMessage(&batch, message); err != nil {
					return BatchDTO{}, n.fail(err)
				}
			}
		}
		if appendRequest != nil && !n.enqueueAppend(appendRequest) {
			n.heldAppend = appendRequest
		}
	}
	if len(n.applyQueue) != 0 && n.applyQueue[0].requiredAppend <= n.durableSequence {
		request := n.applyQueue[0]
		copy(n.applyQueue, n.applyQueue[1:])
		n.applyQueue[len(n.applyQueue)-1] = storageApplyRequest{}
		n.applyQueue = n.applyQueue[:len(n.applyQueue)-1]
		if err := n.applyEntries(request.entries, &batch); err != nil {
			return BatchDTO{}, n.fail(err)
		}
		n.applying = &request
		batch.ApplySequence = request.sequence
		batch.Progress = true
	}
	return batch, nil
}

func appendPeerMessage(batch *BatchDTO, message *pb.Message) error {
	encoded, err := encodePeerMessage(message)
	if err == nil {
		batch.Messages = append(batch.Messages, encoded)
	}
	return err
}

func encodePeerMessage(message *pb.Message) (MessageDTO, error) {
	if etcdraft.IsLocalMsgTarget(message.GetFrom()) || etcdraft.IsLocalMsgTarget(message.GetTo()) {
		return MessageDTO{}, fmt.Errorf("local storage message cannot enter peer transport")
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return MessageDTO{}, fmt.Errorf("marshal outbound message: %w", err)
	}
	if len(data) > maxMessageSize {
		return MessageDTO{}, fmt.Errorf("outbound message exceeds %d bytes", maxMessageSize)
	}
	return MessageDTO{
		From: NodeID(message.GetFrom()), To: NodeID(message.GetTo()), Data: data,
		IsSnapshot: message.GetType() == pb.MsgSnap, Type: message.GetType().String(),
	}, nil
}

func (n *Node) deliverStorageResponses(responses []*pb.Message, batch *BatchDTO) error {
	for _, response := range responses {
		if response.GetTo() == uint64(n.id) {
			if err := n.raw.Step(response); err != nil {
				return fmt.Errorf("step storage completion: %w", err)
			}
		} else if err := appendPeerMessage(batch, response); err != nil {
			return err
		}
	}
	return nil
}

// CompleteApply confirms the consumer applied/restored the returned batch and
// updated its projection. A zero sequence denotes a batch without apply work.
func (n *Node) CompleteApply(sequence uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.stateError(); err != nil {
		return err
	}
	if sequence == 0 {
		return nil
	}
	if n.applying == nil || n.applying.sequence != sequence {
		return ErrApplySequence
	}
	for _, response := range n.applying.responses {
		if response.GetTo() != uint64(n.id) {
			return n.fail(fmt.Errorf("apply completion targets a remote peer"))
		}
		if err := n.raw.Step(response); err != nil {
			return n.fail(fmt.Errorf("step apply completion: %w", err))
		}
	}
	n.applying = nil
	if !n.dynamic && !n.bootstrapped && fixedConfState(n.confState, n.peerIDs) && n.appendPending == 0 && n.heldAppend == nil {
		if err := n.store.createSnapshot(n.appliedIndex, n.confState, n.appliedCommands); err != nil {
			return n.fail(fmt.Errorf("%w: %w", ErrStorage, err))
		}
		n.bootstrapped = true
	}
	return nil
}

func (n *Node) applyEntries(entries []*pb.Entry, batch *BatchDTO) error {
	for _, entry := range entries {
		if entry.GetIndex() <= n.appliedIndex {
			continue
		}
		switch entry.GetType() {
		case pb.EntryNormal:
			if len(entry.GetData()) != 0 {
				commandLimit := maxCommandSize
				if n.dynamic {
					commandLimit = maxDynamicCommandSize
				}
				if len(entry.GetData()) > commandLimit {
					return fmt.Errorf("apply command at index %d: %w", entry.GetIndex(), ErrProposalTooLarge)
				}
				// History is immutable; only the consumer boundary receives a copy.
				n.appliedCommands = append(n.appliedCommands, entry.GetData())
				if n.dynamic {
					n.commandIndexes = append(n.commandIndexes, entry.GetIndex())
					batch.CommandIndexes = append(batch.CommandIndexes, entry.GetIndex())
				}
				batch.Commands = append(batch.Commands, bytes.Clone(entry.GetData()))
			}
		case pb.EntryConfChange, pb.EntryConfChangeV2:
			change, err := decodeConfChange(entry)
			if err != nil {
				return fmt.Errorf("apply configuration at index %d: %w", entry.GetIndex(), err)
			}
			if n.dynamic && entry.GetType() == pb.EntryConfChangeV2 {
				action, memberID, err := validateDynamicConfChange(change)
				if err != nil {
					return fmt.Errorf("apply dynamic configuration at index %d: %w", entry.GetIndex(), err)
				}
				if err := validateDynamicConfStateTransition(n.confState, action, memberID); err != nil {
					return fmt.Errorf("apply dynamic configuration at index %d: %w", entry.GetIndex(), err)
				}
				committedChange := CommittedConfigurationDTO{Index: entry.GetIndex(), OperationID: string(change.GetContext()), MemberID: memberID, Action: action}
				n.configurationChanges = append(n.configurationChanges, committedChange)
				batch.ConfigurationChanges = append(batch.ConfigurationChanges, committedChange)
			}
			expectedConfState, err := applyConfState(n.confState, change)
			if err != nil {
				return fmt.Errorf("apply configuration at index %d: %w", entry.GetIndex(), err)
			}
			appliedConfState := n.raw.ApplyConfChange(change)
			if !confStateEqual(appliedConfState, expectedConfState) {
				return fmt.Errorf("apply configuration at index %d: RawNode ConfState differs from expected", entry.GetIndex())
			}
			n.confState = cloneConfState(appliedConfState)
			n.confChangePending = false
		default:
			return fmt.Errorf("apply unknown entry type %d", entry.GetType())
		}
		n.appliedIndex = entry.GetIndex()
	}
	n.store.appliedIndex = n.appliedIndex
	n.store.confState = n.confState
	n.store.commands = n.appliedCommands
	n.store.commandIndexes = n.commandIndexes
	n.store.configurationChanges = n.configurationChanges
	if n.dynamic {
		n.bootstrapped = containsMember(n.confState, n.id)
		n.confChangePending = n.hasUncommittedConfChange()
	}
	return nil
}
