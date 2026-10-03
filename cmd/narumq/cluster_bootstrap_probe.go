package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	tcpadapter "narumq/internal/adapters/tcp"
)

const (
	clusterBootstrapProbeInterval       = 500 * time.Millisecond
	clusterBootstrapProbeFailpointDelay = 20 * time.Second
)

var errClusterBootstrapVolumeLoss = errors.New("cluster bootstrap refused: a configured peer reports an existing cluster; restore the original volume or replace this member with a new member ID")

type clusterBootstrapProbeFunc func(context.Context, tcpadapter.PeerIdentityDTO) (tcpadapter.BootstrapProbeResultDTO, error)

func clusterDirectoryEmpty(directory string) (bool, error) {
	info, err := os.Stat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect cluster directory: %w", err)
	}
	if !info.IsDir() {
		return false, nil
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return false, fmt.Errorf("inspect cluster directory: %w", err)
	}
	return len(entries) == 0, nil
}

func configuredBootstrapPeerIdentities(config clusterConfigFile) ([]tcpadapter.PeerIdentityDTO, error) {
	_, members, err := clusterBootstrapIdentities(config)
	if err != nil {
		return nil, err
	}
	peers := make([]tcpadapter.PeerIdentityDTO, 0, len(members))
	for index := range members {
		if members[index].MemberID == config.NodeID {
			continue
		}
		peer, err := peerIdentityFromMember(&members[index])
		if err != nil {
			return nil, err
		}
		peers = append(peers, peer)
	}
	return peers, nil
}

func runClusterBootstrapProbe(ctx context.Context, peers []tcpadapter.PeerIdentityDTO, probe clusterBootstrapProbeFunc, interval time.Duration, logger *slog.Logger) ([]uint64, error) {
	pending := slices.Clone(peers)
	slices.SortFunc(pending, func(a, b tcpadapter.PeerIdentityDTO) int {
		if a.MemberID < b.MemberID {
			return -1
		}
		if a.MemberID > b.MemberID {
			return 1
		}
		return 0
	})
	allowed := make([]uint64, 0, len(peers))
	type probeOutcome struct {
		result tcpadapter.BootstrapProbeResultDTO
		err    error
	}
	var lastErr error
	for round := 1; len(pending) > 0; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		outcomes := make([]probeOutcome, len(pending))
		var workers sync.WaitGroup
		for index, peer := range pending {
			workers.Add(1)
			go func() { defer workers.Done(); outcomes[index].result, outcomes[index].err = probe(ctx, peer) }()
		}
		workers.Wait()
		// Refusal takes precedence over all confirmations in the same round.
		for index, outcome := range outcomes {
			if outcome.err == nil && outcome.result.Status == tcpadapter.BootstrapProbeProgressed {
				return nil, fmt.Errorf("%w (member %d term %d commit index %d)", errClusterBootstrapVolumeLoss, pending[index].MemberID, outcome.result.Term, outcome.result.CommitIndex)
			}
		}
		next := pending[:0]
		for index, outcome := range outcomes {
			peer := pending[index]
			if outcome.err != nil {
				lastErr = outcome.err
				next = append(next, peer)
				continue
			}
			status := "ready"
			switch outcome.result.Status {
			case tcpadapter.BootstrapProbeEmpty:
				allowed = append(allowed, peer.MemberID)
				status = "empty"
			case tcpadapter.BootstrapProbeReady:
			default:
				lastErr = errors.New("bootstrap probe response is invalid")
				next = append(next, peer)
				continue
			}
			logger.Info("cluster bootstrap peer confirmed", "peer_id", peer.MemberID, "status", status, "term", outcome.result.Term, "commit_index", outcome.result.CommitIndex)
		}
		pending = next
		if len(pending) == 0 {
			slices.Sort(allowed)
			return allowed, nil
		}
		if round == 1 || round%10 == 0 {
			ids := make([]uint64, len(pending))
			for index, peer := range pending {
				ids[index] = peer.MemberID
			}
			attrs := []any{"pending_peer_ids", ids}
			if lastErr != nil {
				attrs = append(attrs, "error", lastErr.Error())
			}
			logger.Warn("cluster bootstrap probe waiting for peers", attrs...)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
	return allowed, nil
}

func (runtime *clusterRuntime) bootstrapProgress() tcpadapter.BootstrapProgressDTO {
	status := runtime.node.Status()
	return tcpadapter.BootstrapProgressDTO{Term: status.Term, CommitIndex: status.CommitIndex}
}
