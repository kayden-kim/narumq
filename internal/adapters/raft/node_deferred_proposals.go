package raft

import (
	"errors"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type deferredProposal struct {
	message *pb.Message
	bytes   int
}

func (n *Node) canDeferProposal() bool {
	return n.dynamic && n.bootstrapped && containsMember(n.confState, n.id)
}

func normalProposal(message *pb.Message) bool {
	if len(message.Entries) == 0 {
		return false
	}
	for _, entry := range message.Entries {
		if entry.GetType() != pb.EntryNormal || len(entry.GetData()) == 0 {
			return false
		}
	}
	return true
}

// The caller owns message and has already validated it. Retention means only
// that a known pre-append rejection is pending, never commitment or an ACK.
func (n *Node) deferProposal(message *pb.Message, size int) error {
	if n.deferredProposalCount == len(n.deferredProposals) ||
		n.readyInputBytes+n.deferredProposalBytes+size > maxWALFramePayload-1024 ||
		proto.Size(message)+32 > maxMessageSize {
		return ErrBackpressure
	}
	index := (n.deferredProposalHead + n.deferredProposalCount) % len(n.deferredProposals)
	n.deferredProposals[index] = deferredProposal{message: message, bytes: size}
	n.deferredProposalCount++
	n.deferredProposalBytes += size
	return nil
}

// Called only when existing Ready, append and apply work is idle. Accepted
// messages are removed immediately; only ErrProposalDropped keeps the head.
func (n *Node) flushDeferredProposals() (bool, error) {
	if n.deferredProposalCount == 0 {
		return false, nil
	}
	if !n.canDeferProposal() {
		return false, ErrNotLeader
	}
	status := n.raw.BasicStatus()
	if status.Lead == etcdraft.None {
		return false, nil
	}
	messages, bytes := len(n.deferredProposals), maxWALFramePayload-1024
	follower := status.RaftState != etcdraft.StateLeader
	if follower && n.forwardedProposalCapacity != nil {
		messages, bytes = n.forwardedProposalCapacity(NodeID(status.Lead))
	}
	progress := false
	for n.deferredProposalCount != 0 && messages > 0 {
		pending := &n.deferredProposals[n.deferredProposalHead]
		wireBytes := proto.Size(pending.message) + 32
		if wireBytes > maxMessageSize || wireBytes > bytes {
			break
		}
		if err := n.raw.Step(pending.message); err != nil {
			if errors.Is(err, etcdraft.ErrProposalDropped) {
				break
			}
			return progress, err
		}
		n.readyInputBytes += pending.bytes
		n.deferredProposalBytes -= pending.bytes
		*pending = deferredProposal{}
		n.deferredProposalHead = (n.deferredProposalHead + 1) % len(n.deferredProposals)
		n.deferredProposalCount--
		messages--
		bytes -= wireBytes
		progress = true
	}
	return progress, nil
}

func (n *Node) clearDeferredProposals() {
	clear(n.deferredProposals[:])
	n.deferredProposalHead = 0
	n.deferredProposalCount = 0
	n.deferredProposalBytes = 0
}
