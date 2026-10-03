package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	tcpadapter "narumq/internal/adapters/tcp"
)

func TestClusterDirectoryEmpty(t *testing.T) {
	directory := t.TempDir()
	file := filepath.Join(directory, "file")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path string
		want       bool
	}{
		{"missing", filepath.Join(directory, "missing"), true},
		{"empty", t.TempDir(), true},
		{"nonempty", directory, false},
		{"file", file, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := clusterDirectoryEmpty(tc.path)
			if err != nil || got != tc.want {
				t.Fatalf("empty=%t err=%v want=%t", got, err, tc.want)
			}
		})
	}
}

func TestClusterBootstrapProbeWaitsForEveryPeer(t *testing.T) {
	var mu sync.Mutex
	calls := map[uint64]int{}
	probe := func(_ context.Context, peer tcpadapter.PeerIdentityDTO) (tcpadapter.BootstrapProbeResultDTO, error) {
		mu.Lock()
		defer mu.Unlock()
		calls[peer.MemberID]++
		if peer.MemberID == 2 {
			if calls[2] == 1 {
				return tcpadapter.BootstrapProbeResultDTO{}, errors.New("not listening")
			}
			return tcpadapter.BootstrapProbeResultDTO{Status: tcpadapter.BootstrapProbeEmpty}, nil
		}
		return tcpadapter.BootstrapProbeResultDTO{Status: tcpadapter.BootstrapProbeReady, Term: 1, CommitIndex: 3}, nil
	}
	allowed, err := runClusterBootstrapProbe(context.Background(), []tcpadapter.PeerIdentityDTO{{MemberID: 3}, {MemberID: 2}}, probe, time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || !reflect.DeepEqual(allowed, []uint64{2}) || calls[2] != 2 || calls[3] != 1 {
		t.Fatalf("allowed=%v calls=%v err=%v", allowed, calls, err)
	}
}

func TestClusterBootstrapProbeRejectsProgressedPeer(t *testing.T) {
	for _, ids := range [][]uint64{{2, 3}, {3, 2}} {
		peers := make([]tcpadapter.PeerIdentityDTO, len(ids))
		for index, id := range ids {
			peers[index].MemberID = id
		}
		_, err := runClusterBootstrapProbe(context.Background(), peers, func(_ context.Context, peer tcpadapter.PeerIdentityDTO) (tcpadapter.BootstrapProbeResultDTO, error) {
			if peer.MemberID == 3 {
				return tcpadapter.BootstrapProbeResultDTO{Status: tcpadapter.BootstrapProbeProgressed, Term: 2, CommitIndex: 9}, nil
			}
			return tcpadapter.BootstrapProbeResultDTO{Status: tcpadapter.BootstrapProbeEmpty}, nil
		}, time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if !errors.Is(err, errClusterBootstrapVolumeLoss) || !strings.Contains(err.Error(), "member 3") {
			t.Fatalf("refusal=%v", err)
		}
	}
	_, err := runClusterBootstrapProbe(context.Background(), []tcpadapter.PeerIdentityDTO{{MemberID: 3}, {MemberID: 2}}, func(context.Context, tcpadapter.PeerIdentityDTO) (tcpadapter.BootstrapProbeResultDTO, error) {
		return tcpadapter.BootstrapProbeResultDTO{Status: tcpadapter.BootstrapProbeProgressed, Term: 2, CommitIndex: 9}, nil
	}, time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, errClusterBootstrapVolumeLoss) || !strings.Contains(err.Error(), "member 2") {
		t.Fatalf("lowest progressed ID refusal=%v", err)
	}
}

func TestClusterBootstrapProbeStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := runClusterBootstrapProbe(ctx, []tcpadapter.PeerIdentityDTO{{MemberID: 2}}, func(context.Context, tcpadapter.PeerIdentityDTO) (tcpadapter.BootstrapProbeResultDTO, error) {
		cancel()
		return tcpadapter.BootstrapProbeResultDTO{}, errors.New("unavailable")
	}, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
}
