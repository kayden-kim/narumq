package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	raftadapter "narumq/internal/adapters/raft"
	tcpadapter "narumq/internal/adapters/tcp"
)

const raftPeerTickInterval = 100 * time.Millisecond

// runRaftPeerDriver serializes logical ticks and authenticated transport messages
// through one Raft Node. ProcessReady persists before outbound queue admission.
func runRaftPeerDriver(ctx context.Context, node *raftadapter.Node, transport *tcpadapter.TCPPeerTransport, logger *slog.Logger) error {
	if ctx == nil || node == nil || transport == nil {
		return fmt.Errorf("raft peer driver requires context, node, and transport")
	}
	if logger == nil {
		logger = slog.Default()
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- transport.Serve(ctx) }()
	defer transport.Close()

	ticker := time.NewTicker(raftPeerTickInterval)
	defer ticker.Stop()
	previousStatus := raftadapter.StatusDTO{}
	hasPreviousStatus := false
	observeStatus := func() {
		status := node.Status()
		if hasPreviousStatus &&
			status.Role == previousStatus.Role &&
			status.LeaderID == previousStatus.LeaderID &&
			status.Term == previousStatus.Term {
			return
		}
		logger.Info("raft node status changed",
			"node_id", status.NodeID,
			"role", raftRoleName(status.Role),
			"leader_id", status.LeaderID,
			"term", status.Term)
		previousStatus = status
		hasPreviousStatus = true
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-serveResult:
			if ctx.Err() != nil {
				return nil
			}
			if err == nil {
				return fmt.Errorf("raft peer transport stopped unexpectedly")
			}
			return fmt.Errorf("serve raft peer transport: %w", err)
		case message, ok := <-transport.Receive():
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("raft peer transport receive channel closed unexpectedly")
			}
			if err := node.Step(raftadapter.MessageDTO{
				From: raftadapter.NodeID(message.From),
				To:   raftadapter.NodeID(message.To),
				Data: message.Data,
			}); err != nil {
				if errors.Is(err, raftadapter.ErrInvalidMessage) {
					logger.Warn("discarding invalid raft peer message", "peer_id", message.From)
					if err := transport.ResetPeer(message.From); err != nil && ctx.Err() == nil {
						return fmt.Errorf("reset peer %d after invalid Raft message: %w", message.From, err)
					}
					if err := processRaftReady(ctx, node, transport, logger); err != nil {
						return err
					}
					observeStatus()
					continue
				}
				return fmt.Errorf("step raft message from peer %d: %w", message.From, err)
			}
			if err := processRaftReady(ctx, node, transport, logger); err != nil {
				return err
			}
			observeStatus()
		case <-ticker.C:
			if err := node.Tick(); err != nil {
				return fmt.Errorf("tick raft node: %w", err)
			}
			if err := processRaftReady(ctx, node, transport, logger); err != nil {
				return err
			}
			observeStatus()
		}
	}
}

func processRaftReady(ctx context.Context, node *raftadapter.Node, transport *tcpadapter.TCPPeerTransport, logger *slog.Logger) error {
	batch, err := node.ProcessReady()
	if err != nil {
		return fmt.Errorf("process raft Ready: %w", err)
	}
	// Commands are already applied by Node.ProcessReady. They are not MQTT ACKs or readiness.
	return processRaftMessages(ctx, node, transport, logger, batch.Messages)
}

func processRaftMessages(ctx context.Context, node *raftadapter.Node, transport *tcpadapter.TCPPeerTransport, logger *slog.Logger, messages []raftadapter.MessageDTO) error {
	var rejectedPeers [2]raftadapter.NodeID
	rejectedPeerCount := 0
	for _, message := range messages {
		// Once a peer rejects a Ready message, skip later messages to that peer
		// in this batch to preserve FIFO; ReportPeerMessageFailure schedules Raft retry.
		rejected := false
		for index := range rejectedPeerCount {
			if rejectedPeers[index] == message.To {
				rejected = true
				break
			}
		}
		if rejected {
			if message.IsSnapshot {
				if reportErr := node.ReportPeerMessageFailure(message.To, true); reportErr != nil && ctx.Err() == nil {
					return fmt.Errorf("report peer %d snapshot failure after backpressure: %w", message.To, reportErr)
				}
			}
			continue
		}
		if err := transport.Send(tcpadapter.PeerMessageDTO{
			From: uint64(message.From),
			To:   uint64(message.To),
			Data: message.Data,
		}); err != nil {
			if errors.Is(err, tcpadapter.ErrPeerBackpressure) {
				rejectedPeers[rejectedPeerCount] = message.To
				rejectedPeerCount++
				logRaftPeerBackpressure(transport, logger, uint64(message.To))
				if reportErr := node.ReportPeerMessageFailure(message.To, message.IsSnapshot); reportErr != nil && ctx.Err() == nil {
					return fmt.Errorf("report peer %d unreachable after backpressure: %w", message.To, reportErr)
				}
				continue
			}
			if errors.Is(err, tcpadapter.ErrPeerTransportClosed) && ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("queue raft message for peer %d: %w", message.To, err)
		}
	}
	return nil
}

func logRaftPeerBackpressure(transport *tcpadapter.TCPPeerTransport, logger *slog.Logger, peerID uint64) {
	status, err := transport.Status(peerID)
	if err != nil {
		logger.Warn("raft peer transport backpressure", "peer_id", peerID)
		return
	}
	logger.Warn("raft peer transport backpressure", "peer_id", peerID,
		"queued_messages", status.OutboundQueuedMessages,
		"queued_bytes", status.OutboundQueuedBytes,
		"rejected_messages", status.RejectedMessages)
}
func raftRoleName(role raftadapter.Role) string {
	switch role {
	case raftadapter.RoleFollower:
		return "follower"
	case raftadapter.RoleCandidate:
		return "candidate"
	case raftadapter.RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}
