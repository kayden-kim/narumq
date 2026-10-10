package main

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	appmgmt "narumq/internal/application/management"
	managementports "narumq/internal/application/ports/management"
)

type clusterRuntimeNodeSummaryProvider struct {
	managementports.StateProvider
	node *managementports.NodeSummaryDTO
}

func (provider *clusterRuntimeNodeSummaryProvider) GetNodeSummary(context.Context, string) (*managementports.NodeSummaryDTO, error) {
	return provider.node, nil
}

func TestClusterStateProviderValidatesNodeQueriesAndPaginatesFilteredResults(t *testing.T) {
	runtimes := newReadAdmissionTestCluster(t)
	machine := readAdmissionTestMachine(t, runtimes)
	for _, runtime := range runtimes {
		runtime.machine = machine
		runtime.storage = newClusterStorageMonitor(time.Now, nil)
	}

	ctx := context.Background()
	base := &clusterRuntimeNodeSummaryProvider{}
	provider := &clusterStateProvider{StateProvider: base, runtime: runtimes[0]}
	if _, err := provider.GetNodeSummary(ctx, "not-a-node-id"); !errors.Is(err, appmgmt.ErrNodeNotFound) {
		t.Fatalf("GetNodeSummary(non-numeric) error = %v, want node not found", err)
	}
	if _, err := provider.GetNodeSummary(ctx, "99"); !errors.Is(err, appmgmt.ErrNodeNotFound) {
		t.Fatalf("GetNodeSummary(missing) error = %v, want node not found", err)
	}

	base.node = &managementports.NodeSummaryDTO{
		State: managementports.NodeStateReady, Ready: true, ConnectionCount: 7, UptimeMillis: 1234,
	}
	local, err := provider.GetNodeSummary(ctx, "1")
	if err != nil || local.State != managementports.NodeStateReady || !local.Ready || local.ConnectionCount != 7 || local.UptimeMillis != 1234 {
		t.Fatalf("GetNodeSummary(local) = (%+v, %v), want base node state", local, err)
	}
	base.node = nil
	local, err = provider.GetNodeSummary(ctx, "1")
	if err != nil || local.State != managementports.NodeStateJoining || local.Ready {
		t.Fatalf("GetNodeSummary(local without base state) = (%+v, %v), want joining and not ready", local, err)
	}

	for _, limit := range []int{-1, appmgmt.MaxPageLimit + 1} {
		if _, _, err := provider.ListNodeSummaries(ctx, managementports.NodeFilter{Limit: limit}); !errors.Is(err, appmgmt.ErrInvalidPageSize) {
			t.Errorf("ListNodeSummaries(limit=%d) error = %v, want invalid page size", limit, err)
		}
	}
	for _, cursor := range []string{"!", encodeClusterCursor(0)[:len(encodeClusterCursor(0))-1], encodeClusterCursor(-1)} {
		if _, _, err := provider.ListNodeSummaries(ctx, managementports.NodeFilter{Cursor: cursor}); !errors.Is(err, appmgmt.ErrInvalidCursor) {
			t.Errorf("ListNodeSummaries(cursor=%q) error = %v, want invalid cursor", cursor, err)
		}
	}

	base.node = &managementports.NodeSummaryDTO{State: managementports.NodeStateReady, Ready: true}

	joining, page, err := provider.ListNodeSummaries(ctx, managementports.NodeFilter{State: managementports.NodeStateJoining, Limit: 1})
	if err != nil {
		t.Fatalf("ListNodeSummaries(first joining page): %v", err)
	}
	if len(joining) != 1 || joining[0].NodeID != "2" || !page.HasMore || page.NextCursor == nil {
		t.Fatalf("first joining page = (%+v, %+v), want node 2 and next page", joining, page)
	}
	offset, err := decodeClusterCursor(*page.NextCursor)
	if err != nil || offset != 1 {
		t.Fatalf("next cursor offset = %d, %v; want 1", offset, err)
	}
	joining, page, err = provider.ListNodeSummaries(ctx, managementports.NodeFilter{State: managementports.NodeStateJoining, Limit: 1, Cursor: *page.NextCursor})
	if err != nil {
		t.Fatalf("ListNodeSummaries(second joining page): %v", err)
	}
	if len(joining) != 1 || joining[0].NodeID != "3" || page.HasMore || page.NextCursor != nil {
		t.Fatalf("second joining page = (%+v, %+v), want node 3 and terminal page", joining, page)
	}

	all, page, err := provider.ListNodeSummaries(ctx, managementports.NodeFilter{})
	if err != nil || len(all) != 3 || page.HasMore {
		t.Fatalf("ListNodeSummaries(default limit) = (%d items, %+v, %v), want all three", len(all), page, err)
	}
	beyond, page, err := provider.ListNodeSummaries(ctx, managementports.NodeFilter{Limit: 1, Cursor: encodeClusterCursor(100)})
	if err != nil || len(beyond) != 0 || page.HasMore {
		t.Fatalf("ListNodeSummaries(offset past end) = (%+v, %+v, %v), want empty final page", beyond, page, err)
	}
}

func TestDecodeClusterCursorRejectsMalformedValues(t *testing.T) {
	for _, cursor := range []string{
		"!",
		encodeClusterCursor(0)[:len(encodeClusterCursor(0))-1],
		base64.RawURLEncoding.EncodeToString([]byte("not-a-cursor")),
		base64.RawURLEncoding.EncodeToString([]byte("cur:not-a-number")),
		encodeClusterCursor(-1),
	} {
		if offset, err := decodeClusterCursor(cursor); err == nil {
			t.Errorf("decodeClusterCursor(%q) = %d, nil; want invalid cursor", cursor, offset)
		}
	}
	if offset, err := decodeClusterCursor(encodeClusterCursor(7)); err != nil || offset != 7 {
		t.Fatalf("decodeClusterCursor(valid) = %d, %v; want 7", offset, err)
	}
	if offset, err := decodeClusterCursor(encodeClusterCursor(0)); err != nil || offset != 0 {
		t.Fatalf("decodeClusterCursor(zero) = %d, %v; want 0", offset, err)
	}
}
