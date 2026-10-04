package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	drainClusterCaseTimeout = 4 * time.Minute
	malformedCaseTimeout    = 90 * time.Second
)

// RunDrain executes the fixed cluster-drain and malformed DISCONNECT suite.
func RunDrain(ctx context.Context, options runOptions) (Summary, error) {
	p, _, err := LoadDrainProfile(options.ProfilePath)
	if err != nil {
		return Summary{}, err
	}
	ids := make([]string, 0, len(p.ExpectedCases))
	cases := make(map[string]DrainExpectedCase, len(p.ExpectedCases))
	for _, c := range p.ExpectedCases {
		ids = append(ids, c.CaseID)
		cases[c.CaseID] = c
	}
	return runSuite(ctx, options, suiteSpec{profile: p, caseIDs: ids,
		runCase: func(ctx context.Context, run *compatibilityRun, id string) error {
			c := cases[id]
			if c.Kind == "cluster_drain" {
				return run.runDrainClusterCase(ctx, p, c)
			}
			return run.runMalformedCase(ctx, p, c)
		},
		summarize: func(run *compatibilityRun, start, finish string) Summary {
			return AggregateDrainSummary(options.RunID, p, run.results, run.unexpected, start, finish)
		},
	})
}

func (run *compatibilityRun) cleanupDrainCase() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run.cleanup(ctx)
	run.containerMu.Lock()
	run.containers = nil
	run.networks = nil
	run.containerMu.Unlock()
}

func (run *compatibilityRun) drainCaseDirectory(c DrainExpectedCase, subdirectories ...string) (string, error) {
	dir := filepath.Join(run.outputDir, "cases", c.CaseID)
	if !pathWithin(run.outputDir, dir) {
		return "", errors.New("drain case escapes output directory")
	}
	for _, sub := range subdirectories {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0755); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func (run *compatibilityRun) startDrainClient(ctx context.Context, c DrainExpectedCase, network, address, credentials, roleDir string, detached bool) ([]byte, error) {
	name := containerName(run.options.RunID, c.CaseID, "client")
	args := []string{"run"}
	if detached {
		args = append(args, "--detach")
		run.registerContainer(name)
	} else {
		args = append(args, "--rm")
	}
	args = append(args, "--name", name, "--network", network, "--cpus", "1", "--memory", "512m", "--read-only", "--tmpfs", "/tmp:size=64m",
		"--mount", "type=bind,source="+hostPath(run.options.ProfilePath)+",destination=/input/profile.json,readonly",
		"--mount", "type=bind,source="+hostPath(credentials)+",destination=/input/credentials.json,readonly",
		"--mount", "type=bind,source="+hostPath(roleDir)+",destination=/output",
		run.images[c.ExpectedResults[0].SDK], "--suite", "drain", "--case", c.CaseID, "--role", "client", "--address", address, "--profile", "/input/profile.json", "--output", "/output")
	return run.command(ctx, "docker", args...)
}

func (run *compatibilityRun) collectDrainCaseResults(p DrainProfile, c DrainExpectedCase, dir string) error {
	results := make([]Result, 0, len(c.ExpectedResults))
	for _, role := range c.ExpectedResults {
		data, err := os.ReadFile(filepath.Join(dir, role.Role, "result.json"))
		if err != nil {
			run.results[c.CaseID] = results
			return err
		}
		result, err := DecodeResult(data)
		if err != nil {
			return err
		}
		results = append(results, result)
	}
	run.results[c.CaseID] = results
	return ValidateDrainCaseResults(p, c, results)
}

func drainPoll(ctx context.Context, timeout time.Duration, probe func() (bool, error)) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for {
		ok, err := probe()
		if ok && err == nil {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-deadlineCtx.Done():
			return errors.Join(deadlineCtx.Err(), lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (run *compatibilityRun) runMalformedCase(parent context.Context, p DrainProfile, c DrainExpectedCase) error {
	ctx, cancel := context.WithTimeout(parent, malformedCaseTimeout)
	defer cancel()
	dir, err := run.drainCaseDirectory(c, "client", "fakeserver")
	if err != nil {
		return err
	}
	defer run.cleanupDrainCase()
	credentials := filepath.Join(dir, "credentials.json")
	if err := writeJSON(credentials, map[string]string{"username": "jwt", "password": "test-only-fake-server"}); err != nil {
		return err
	}
	defer os.Remove(credentials)
	fake := containerName(run.options.RunID, c.CaseID, "fake")
	run.registerContainer(fake)
	traceDir := filepath.Join(dir, "fakeserver")
	tracePath := filepath.Join(traceDir, "fakeserver-trace.jsonl")
	args := []string{"run", "--detach", "--name", fake, "--cpus", "0.25", "--memory", "64m", "--read-only", "--mount", "type=bind,source=" + hostPath(traceDir) + ",destination=/output", "--entrypoint", "/fakeserver", run.images["go"], "--listen", "127.0.0.1:1883", "--trap", "127.0.0.1:1886", "--vector", c.Vector, "--trace", "/output/fakeserver-trace.jsonl"}
	if _, err := run.command(ctx, "docker", args...); err != nil {
		return err
	}
	if err := drainPoll(ctx, 10*time.Second, func() (bool, error) {
		events, err := decodeTrace[fakeServerTraceEvent](tracePath)
		if err != nil {
			return false, err
		}
		listening := 0
		for _, event := range events {
			if event.Event == "listening" {
				listening++
			}
		}
		return listening == 2, nil
	}); err != nil {
		return err
	}
	roleDir := filepath.Join(dir, "client")
	output, clientErr := run.startDrainClient(ctx, c, "container:"+fake, p.FakeServer.Endpoint, credentials, roleDir, false)
	if err := os.WriteFile(filepath.Join(roleDir, "client-stdout-stderr.log"), output, 0644); err != nil {
		return err
	}
	resultErr := run.collectDrainCaseResults(p, c, dir)
	events, traceErr := decodeTrace[fakeServerTraceEvent](tracePath)
	if traceErr == nil {
		traceErr = validateFakeServerTrace(events, c.Vector, p.ClientIDs.Client)
	}
	return errors.Join(clientErr, resultErr, traceErr)
}

func drainHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
}
func drainRequest(ctx context.Context, client *http.Client, method, address, path, token, key string, body any) (int, []byte, error) {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+address+path, bytes.NewReader(encoded))
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return response.StatusCode, data, err
}

func (run *compatibilityRun) runDrainClusterCase(parent context.Context, p DrainProfile, c DrainExpectedCase) error {
	ctx, cancel := context.WithTimeout(parent, drainClusterCaseTimeout)
	defer cancel()
	dir, err := run.drainCaseDirectory(c, "client", "lb", "node1", "node2", "node3")
	if err != nil {
		return err
	}
	fixture, err := createDrainFixture()
	if err != nil {
		return err
	}
	defer fixture.close()
	defer run.cleanupDrainCase()
	network := "narumq-compat-" + run.options.RunID + "-" + c.CaseID
	run.registerNetwork(network)
	if _, err := run.command(ctx, "docker", "network", "create", network); err != nil {
		return err
	}
	management := make(map[string]string, 3)
	for _, node := range fixture.Nodes {
		owner := containerName(run.options.RunID, c.CaseID, "node"+node.ID+"-net")
		broker := containerName(run.options.RunID, c.CaseID, "node"+node.ID)
		run.registerContainer(owner)
		run.registerContainer(broker)
		defer func() {
			artifactCtx, artifactCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer artifactCancel()
			for _, entry := range []struct{ command, file string }{{"logs", "broker.log"}, {"inspect", "broker-inspect.json"}} {
				output, err := run.command(artifactCtx, "docker", entry.command, broker)
				if err == nil {
					_ = os.WriteFile(filepath.Join(dir, "node"+node.ID, entry.file), output, 0644)
				}
			}
		}()
		mount := "type=bind,source=" + hostPath(node.ConfigDir) + ",destination=/config,readonly"
		ownerArgs := []string{"run", "--detach", "--name", owner, "--network", network, "--network-alias", node.DNSName, "--hostname", node.DNSName, "--publish", "127.0.0.1::8080", "--cpus", "0.25", "--memory", "64m", "--read-only", "--mount", mount, "--entrypoint", "/jwks", run.images["go"], "--file", "/config/jwks.json", "--listen", "127.0.0.1:18080"}
		if _, err := run.command(ctx, "docker", ownerArgs...); err != nil {
			return err
		}
		brokerArgs := []string{"run", "--detach", "--name", broker, "--network", "container:" + owner, "--cpus", "1", "--memory", "512m", "--read-only", "--tmpfs", "/tmp:size=64m", "--tmpfs", "/var/lib/narumq:size=256m", "--mount", mount, run.images["broker"], "-cluster-config", "/config/node.json"}
		if _, err := run.command(ctx, "docker", brokerArgs...); err != nil {
			return err
		}
		output, err := run.command(ctx, "docker", "port", owner, "8080/tcp")
		if err != nil {
			return err
		}
		for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "127.0.0.1:") {
				management[node.ID] = line
				break
			}
		}
		if management[node.ID] == "" {
			return fmt.Errorf("node %s has no loopback management port", node.ID)
		}
	}
	client := drainHTTPClient()
	defer client.CloseIdleConnections()
	if err := drainPoll(ctx, 90*time.Second, func() (bool, error) {
		for _, node := range fixture.Nodes {
			status, _, err := drainRequest(ctx, client, http.MethodGet, management[node.ID], "/health/ready", "", "", nil)
			if err != nil || status != 200 {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("wait cluster readiness: %w", err)
	}
	lb := containerName(run.options.RunID, c.CaseID, "lb")
	run.registerContainer(lb)
	lbDir := filepath.Join(dir, "lb")
	lbTrace := filepath.Join(lbDir, "lb-trace.jsonl")
	lbArgs := []string{"run", "--detach", "--name", lb, "--network", network, "--network-alias", "lb.compat.test", "--cpus", "0.25", "--memory", "64m", "--read-only", "--mount", "type=bind,source=" + hostPath(lbDir) + ",destination=/output", "--entrypoint", "/lb", run.images["go"], "--listen", "0.0.0.0:1883"}
	for _, id := range []int{3, 1, 2} {
		host := fmt.Sprintf("node%d.compat.test", id)
		lbArgs = append(lbArgs, "--backend", host+":1883,http://"+host+":8080/health/ready")
	}
	lbArgs = append(lbArgs, "--health-interval-ms", "100", "--trace", "/output/lb-trace.jsonl")
	if _, err := run.command(ctx, "docker", lbArgs...); err != nil {
		return err
	}
	if err := drainPoll(ctx, 10*time.Second, func() (bool, error) {
		events, err := decodeTrace[lbTraceEvent](lbTrace)
		if err != nil {
			return false, err
		}
		ready := map[string]bool{}
		for _, event := range events {
			if event.Event == "health" {
				ready[event.Backend] = event.Ready
			}
		}
		return ready["node1.compat.test:1883"] && ready["node2.compat.test:1883"] && ready["node3.compat.test:1883"], nil
	}); err != nil {
		return err
	}
	roleDir := filepath.Join(dir, "client")
	if _, err := run.startDrainClient(ctx, c, network, p.Cluster.CommonEndpoint, filepath.Join(fixture.Directory, "client", "credentials.json"), roleDir, true); err != nil {
		return err
	}
	driver := containerName(run.options.RunID, c.CaseID, "client")
	defer func() {
		logsCtx, logsCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer logsCancel()
		output, err := run.command(logsCtx, "docker", "logs", driver)
		if err == nil {
			_ = os.WriteFile(filepath.Join(roleDir, "client-stdout-stderr.log"), output, 0644)
		}
	}()
	readyCtx, readyCancel := context.WithTimeout(ctx, time.Duration(p.OperationTimeoutMillis)*time.Millisecond)
	defer readyCancel()
	if err := waitForFile(readyCtx, filepath.Join(roleDir, "drain-ready.json")); err != nil {
		return errors.Join(fmt.Errorf("wait drain driver readiness: %w", err), run.collectDrainCaseResults(p, c, dir))
	}
	var readiness crossReadiness
	if err := decodeDrainReadiness(filepath.Join(roleDir, "drain-ready.json"), c, &readiness); err != nil {
		return err
	}
	events, err := decodeTrace[lbTraceEvent](lbTrace)
	if err != nil {
		return err
	}
	if err := validateLBInitialConnect(events, fmt.Sprintf("node%d.compat.test:1883", p.Cluster.DrainNodeID)); err != nil {
		return err
	}
	if err := startDrainOperation(ctx, client, management, fixture.AdminToken, p, c); err != nil {
		return err
	}
	output, waitErr := run.command(ctx, "docker", "wait", driver)
	if waitErr == nil && strings.TrimSpace(string(output)) != "0" {
		waitErr = fmt.Errorf("drain SDK exited %s", strings.TrimSpace(string(output)))
	}
	var lastOperation []byte
	operationErr := drainPoll(ctx, 60*time.Second, func() (bool, error) {
		status, data, err := drainRequest(ctx, client, http.MethodGet, management["1"], "/api/v1/nodes/"+strconv.Itoa(p.Cluster.DrainNodeID)+"/drain", fixture.AdminToken, "", nil)
		if len(data) > 0 {
			lastOperation = data
		}
		if err != nil || status != 200 {
			return false, err
		}
		var response struct {
			Data struct {
				State string `json:"state"`
				Phase string `json:"phase"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &response); err != nil {
			return false, err
		}
		return response.Data.Phase == "COMPLETED" && response.Data.State == "SAFE_TO_STOP", nil
	})
	if len(lastOperation) > 0 {
		if err := os.WriteFile(filepath.Join(dir, "drain-operation.json"), lastOperation, 0644); err != nil {
			return err
		}
	}
	return errors.Join(waitErr, operationErr, run.collectDrainCaseResults(p, c, dir))
}

func decodeDrainReadiness(path string, c DrainExpectedCase, value *crossReadiness) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("drain readiness contains trailing data")
	}
	if value.SchemaVersion != 1 || value.CaseID != c.CaseID || value.SDK != c.ExpectedResults[0].SDK || value.Role != "client" || value.Protocol != "mqtt5" || !value.Ready {
		return errors.New("drain readiness identity mismatch")
	}
	return nil
}

func startDrainOperation(ctx context.Context, client *http.Client, addresses map[string]string, token string, p DrainProfile, c DrainExpectedCase) error {
	body := map[string]any{"serverReference": p.Cluster.ServerReference, "connectionRatePerSecond": p.Cluster.ConnectionRatePerSecond, "loadBalancerPropagationDelayMs": p.Cluster.LoadBalancerPropagationDelayMillis, "timeoutMs": p.Cluster.DrainTimeoutMillis}
	retryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		status, data, err := drainRequest(retryCtx, client, http.MethodGet, addresses["1"], "/api/v1/cluster", token, "", nil)
		if err == nil && status == 200 {
			var response struct {
				Data struct {
					LeaderNodeID string `json:"leaderNodeId"`
				} `json:"data"`
			}
			if err := json.Unmarshal(data, &response); err != nil {
				return err
			}
			address, ok := addresses[response.Data.LeaderNodeID]
			if ok {
				status, data, err = drainRequest(retryCtx, client, http.MethodPost, address, "/api/v1/nodes/"+strconv.Itoa(p.Cluster.DrainNodeID)+"/drain", token, "compat-drain-"+c.CaseID, body)
				if err == nil && status == http.StatusAccepted {
					return nil
				}
				if err == nil && status < 500 {
					return fmt.Errorf("drain POST status %d: %s", status, data)
				}
			}
		}
		select {
		case <-retryCtx.Done():
			return fmt.Errorf("start drain operation: %w", retryCtx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
