package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func healthy() *string { s := "HEALTHY"; return &s }

func TestSettledRequiresOneLeaderHealthyFollowersAndAppliedWatermark(t *testing.T) {
	page := func(nodes ...mgmtNode) mgmtNodePage { return mgmtNodePage{Data: nodes} }
	leader := mgmtNode{NodeID: "1", Role: "LEADER", LastAppliedIndex: 100}
	follower := func(id string, applied int64, health *string) mgmtNode {
		return mgmtNode{NodeID: id, Role: "FOLLOWER", LastAppliedIndex: applied, ReplicationHealth: health}
	}
	var watermark int64
	if ok, reason := settled(page(leader, follower("2", 100, healthy()), follower("3", 101, healthy())), &watermark); !ok || watermark != 100 {
		t.Fatalf("settled cluster rejected: %s (watermark %d)", reason, watermark)
	}
	morePages := page(leader, follower("2", 100, healthy()), follower("3", 100, healthy()))
	morePages.Page.HasMore = true
	cases := map[string]mgmtNodePage{
		"two nodes":        page(leader, follower("2", 100, healthy())),
		"no leader":        page(follower("1", 100, healthy()), follower("2", 100, healthy()), follower("3", 100, healthy())),
		"unhealthy":        page(leader, follower("2", 100, nil), follower("3", 100, healthy())),
		"behind watermark": page(leader, follower("2", 99, healthy()), follower("3", 100, healthy())),
		"more pages":       morePages,
	}
	for name, p := range cases {
		watermark = 0
		if ok, _ := settled(p, &watermark); ok {
			t.Errorf("%s: cluster reported settled", name)
		}
	}
}

func TestEnsureFeaturesActivatesAndVerifiesEveryRequiredFeature(t *testing.T) {
	var mu sync.Mutex
	var posted []string
	active := []string{"cluster-policy", "session-metadata", "proposal-recovery", "shared-subscription-reroute"}
	failFirstPost := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodPost {
			if failFirstPost {
				failFirstPost = false
				http.Error(w, "not leader", http.StatusServiceUnavailable)
				return
			}
			posted = append(posted, strings.TrimPrefix(r.URL.Path, "/api/v1/cluster/features/"))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"activeFeatures": active}})
	}))
	defer server.Close()
	client := newMgmtClient(server.URL, "secret", nil)
	got, err := client.ensureFeatures(context.Background(), requiredClusterFeatures, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(posted, requiredClusterFeatures) {
		t.Fatalf("activated %v after a refused first attempt, want %v", posted, requiredClusterFeatures)
	}
	if !slices.Equal(got, []string{"cluster-policy", "proposal-recovery", "session-metadata", "shared-subscription-reroute"}) {
		t.Fatalf("active features %v, want the sorted active list", got)
	}

	mu.Lock()
	active = []string{"session-metadata"}
	mu.Unlock()
	if _, err := client.ensureFeatures(context.Background(), requiredClusterFeatures, 10*time.Second); err == nil ||
		!strings.Contains(err.Error(), "proposal-recovery was not active") {
		t.Fatalf("missing proposal-recovery: error %v, want it reported as not active", err)
	}
}
