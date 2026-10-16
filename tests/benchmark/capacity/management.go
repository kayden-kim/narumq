package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"
)

// mgmtClient reads NaruMQ management state through one (possibly load-balanced) endpoint.
type mgmtClient struct {
	base  string
	token string
	http  *http.Client
}

func newMgmtClient(base, token string, tlsConfig *tls.Config) *mgmtClient {
	transport := &http.Transport{DisableKeepAlives: true, TLSClientConfig: tlsConfig}
	return &mgmtClient{
		base: strings.TrimRight(base, "/"), token: token,
		http: &http.Client{Timeout: 5 * time.Second, Transport: transport},
	}
}

func readSecretFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return token, nil
}

type mgmtNode struct {
	NodeID            string  `json:"nodeId"`
	Role              string  `json:"role"`
	LastAppliedIndex  int64   `json:"lastAppliedIndex"`
	ReplicationHealth *string `json:"replicationHealth"`
}

type mgmtNodePage struct {
	Data []mgmtNode `json:"data"`
	Page struct {
		HasMore bool `json:"hasMore"`
	} `json:"page"`
}

// requiredClusterFeatures are activated and verified before setup. BP-06 activates the first two;
// shared-subscription-reroute (ADR-0060) keeps shared-group messages when a member's session ends.
var requiredClusterFeatures = []string{"session-metadata", "proposal-recovery", "shared-subscription-reroute"}

// ensureFeatures activates each feature (idempotent POST through the endpoint, retried until
// the timeout because the endpoint may hit a node that cannot serve it yet) and then verifies
// all of them are listed in activeFeatures. It returns the sorted active feature list.
func (m *mgmtClient) ensureFeatures(ctx context.Context, features []string, timeout time.Duration) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for _, feature := range features {
		var last error
		for {
			if last = m.post(ctx, "/api/v1/cluster/features/"+feature); last == nil {
				break
			}
			if !sleepCtx(ctx, 500*time.Millisecond) {
				return nil, fmt.Errorf("activate %s: %w", feature, last)
			}
		}
	}
	var body struct {
		Data struct {
			ActiveFeatures []string `json:"activeFeatures"`
		} `json:"data"`
	}
	if err := m.get(ctx, "/api/v1/cluster/features", &body); err != nil {
		return nil, fmt.Errorf("read cluster features: %w", err)
	}
	if err := verifyFeaturesActive(features, body.Data.ActiveFeatures); err != nil {
		return nil, err
	}
	active := append([]string{}, body.Data.ActiveFeatures...)
	slices.Sort(active)
	return active, nil
}

// verifyFeaturesActive fails when any required feature is missing from the active list.
func verifyFeaturesActive(required, active []string) error {
	for _, feature := range required {
		if !slices.Contains(active, feature) {
			return fmt.Errorf("%s was not active after activation (active: %v)", feature, active)
		}
	}
	return nil
}

func (m *mgmtClient) post(ctx context.Context, path string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, m.base+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+m.token)
	response, err := m.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s: HTTP %d", path, response.StatusCode)
	}
	return nil
}

// ready probes /health/ready on the node that answers this endpoint.
func (m *mgmtClient) ready(ctx context.Context) error {
	return m.get(ctx, "/health/ready", nil)
}

func (m *mgmtClient) get(ctx context.Context, path string, into any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.base+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+m.token)
	response, err := m.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return fmt.Errorf("GET %s: HTTP %d", path, response.StatusCode)
	}
	if into == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}
	return json.NewDecoder(response.Body).Decode(into)
}

// leader returns the current Raft leader node ID.
func (m *mgmtClient) leader(ctx context.Context) (string, error) {
	var body struct {
		Data struct {
			LeaderNodeID *string `json:"leaderNodeId"`
		} `json:"data"`
	}
	if err := m.get(ctx, "/api/v1/cluster", &body); err != nil {
		return "", err
	}
	if body.Data.LeaderNodeID == nil || *body.Data.LeaderNodeID == "" {
		return "", errors.New("cluster reports no leader")
	}
	return *body.Data.LeaderNodeID, nil
}

// leaderWithRetry retries until the deadline because the endpoint may hit a node that is down.
func (m *mgmtClient) leaderWithRetry(ctx context.Context, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last error
	for {
		leader, err := m.leader(ctx)
		if err == nil {
			return leader, nil
		}
		last = err
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("leader discovery: %w: %v", ctx.Err(), last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// settled evaluates the §3.6 NaruMQ resync rule against one nodes page: three nodes, exactly
// one leader, every follower HEALTHY, and every applied index at or above the leader watermark.
// The nodes API reports readiness only for the node that answers, so per-node readiness is
// probed separately through /health/ready. A zero watermark means the caller has not captured one yet.
func settled(page mgmtNodePage, watermark *int64) (bool, string) {
	if page.Page.HasMore || len(page.Data) != 3 {
		return false, fmt.Sprintf("expected one page of 3 nodes, got %d (hasMore=%t)", len(page.Data), page.Page.HasMore)
	}
	leaders := 0
	var leader mgmtNode
	for _, node := range page.Data {
		if node.Role == "LEADER" {
			leaders++
			leader = node
		}
	}
	if leaders != 1 {
		return false, fmt.Sprintf("expected exactly one leader, got %d", leaders)
	}
	if *watermark == 0 {
		*watermark = leader.LastAppliedIndex
	}
	for _, node := range page.Data {
		if node.Role != "LEADER" && (node.ReplicationHealth == nil || *node.ReplicationHealth != "HEALTHY") {
			return false, "node " + node.NodeID + " replication is not HEALTHY"
		}
		if node.LastAppliedIndex < *watermark {
			return false, fmt.Sprintf("node %s applied index %d is below watermark %d", node.NodeID, node.LastAppliedIndex, *watermark)
		}
	}
	return true, ""
}

// waitSettled polls until the cluster is formed and resynced or the timeout expires.
func (m *mgmtClient) waitSettled(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var watermark int64
	last := "no response"
	for {
		var page mgmtNodePage
		if err := m.get(ctx, "/api/v1/nodes", &page); err != nil {
			last = err.Error()
		} else if ok, reason := settled(page, &watermark); !ok {
			last = reason
		} else if err := m.ready(ctx); err != nil {
			last = err.Error()
		} else {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster not settled within %s: %s", timeout, last)
		case <-time.After(250 * time.Millisecond):
		}
	}
}
