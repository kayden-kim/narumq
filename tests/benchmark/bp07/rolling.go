package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"narumq/tests/benchmark/internal/environment"
	"narumq/tests/benchmark/internal/jsonl"
)

var willOutcomeReasons = []string{"suppressed_normal_disconnect", "suppressed_reconnected_within_delay", "expected_mqtt311_server_disconnect", "expected_zero_or_expired_will_delay", "unexpected_supported_client", "unplanned_failure"}

type roundPlan struct {
	Round  int
	Node   string
	NodeID int
}
type replacementDriver interface {
	start(context.Context, roundPlan) error
	completed(context.Context, roundPlan) (bool, string, error)
	restart(context.Context, roundPlan) error
	ready(context.Context, roundPlan) error
	scrape(context.Context, string) error
}
type narumqRoundDetail struct {
	OperationID     string `json:"operationId"`
	ServerStartedAt string `json:"serverStartedAt"`
	FinalPhase      string `json:"finalPhase"`
}
type emqxRoundDetail struct {
	FinalState string `json:"finalState"`
}
type rollingRoundSummary struct {
	Round              int                `json:"round"`
	Node               string             `json:"node"`
	DrainMillis        float64            `json:"drainMillis"`
	RestartReadyMillis float64            `json:"restartReadyMillis"`
	NaruMQ             *narumqRoundDetail `json:"narumq,omitempty"`
	EMQX               *emqxRoundDetail   `json:"emqx,omitempty"`
	WillOutcomeDelta   map[string]int64   `json:"willOutcomeDelta,omitempty"`
}
type rollingSummary struct {
	SchemaVersion     int                   `json:"schemaVersion"`
	Target            string                `json:"target"`
	Valid             bool                  `json:"valid"`
	FailureReasons    []string              `json:"failureReasons"`
	Rounds            []rollingRoundSummary `json:"rounds"`
	WillOutcomeTotals map[string]int64      `json:"willOutcomeTotals"`
}
type rollingEvent struct {
	Round      int    `json:"round"`
	Node       string `json:"node"`
	Step       string `json:"step"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
	Outcome    string `json:"outcome"`
	Detail     string `json:"detail"`
}

func newRollingController(target string, profile Profile, runID, artifactDir string, fixture *bp07Fixture, options environment.ClusterTargetOptions) func(context.Context, []environment.ClusterNodeResult) error {
	return func(ctx context.Context, nodes []environment.ClusterNodeResult) (runErr error) {
		summary := rollingSummary{SchemaVersion: 1, Target: target, Valid: true, FailureReasons: []string{}, Rounds: []rollingRoundSummary{}, WillOutcomeTotals: make(map[string]int64)}
		defer func() {
			if runErr != nil {
				summary.Valid = false
				summary.FailureReasons = append(summary.FailureReasons, runErr.Error())
			}
			if err := writeAtomicJSON(filepath.Join(artifactDir, "rolling-summary.json"), summary); err != nil {
				runErr = errors.Join(runErr, err)
			}
			status := "success"
			reason := ""
			if runErr != nil {
				status = "failed"
				reason = runErr.Error()
			}
			err := writeAtomicJSON(filepath.Join(artifactDir, "control", "rolling-complete.json"), rollingCompletion{Status: status, Reason: reason, WrittenAt: time.Now().UTC().Format(time.RFC3339Nano)})
			runErr = errors.Join(runErr, err)
		}()
		log, err := jsonl.Open(filepath.Join(artifactDir, "rolling.jsonl"))
		if err != nil {
			return err
		}
		defer func() { runErr = errors.Join(runErr, log.Close()) }()
		var driver replacementDriver
		if target == "narumq" {
			n, err := newNaruMQDriver(profile, runID, artifactDir, fixture.ManagementToken, nodes)
			if err != nil {
				return err
			}
			driver = n
			for _, reason := range willOutcomeReasons {
				summary.WillOutcomeTotals[reason] = 0
			}
		} else if target == "emqx" {
			driver = &emqxDriver{profile: profile, artifactDir: artifactDir, nodes: nodes, options: options}
		} else {
			return fmt.Errorf("unsupported rolling target %q", target)
		}
		for {
			var ready struct {
				WrittenAt   string `json:"writtenAt"`
				Connections int    `json:"connections"`
			}
			if readControlJSON(filepath.Join(artifactDir, "control", "workload-ready.json"), &ready) == nil {
				if ready.Connections != profile.clients() {
					return fmt.Errorf("workload ready connections %d, want %d", ready.Connections, profile.clients())
				}
				break
			}
			if err := waitForContext(ctx, time.Duration(profile.HealthPollMillis)*time.Millisecond); err != nil {
				return err
			}
		}
		for cycle := 0; cycle < profile.Cycles; cycle++ {
			for _, id := range profile.ReplacementOrder {
				round := roundPlan{Round: len(summary.Rounds) + 1, Node: fmt.Sprintf("node%d", id), NodeID: id}
				entry := rollingRoundSummary{Round: round.Round, Node: round.Node}
				// Keep even incomplete rounds in the summary for failure diagnosis.
				summary.Rounds = append(summary.Rounds, entry)
				entryPtr := &summary.Rounds[len(summary.Rounds)-1]
				step := func(name string, execute func() (string, error)) error {
					start := time.Now()
					detail, err := execute()
					outcome := "success"
					if err != nil {
						outcome = "failed"
						if detail == "" {
							detail = err.Error()
						}
					}
					writeErr := log.Write(rollingEvent{Round: round.Round, Node: round.Node, Step: name, StartedAt: start.UTC().Format(time.RFC3339Nano), FinishedAt: time.Now().UTC().Format(time.RFC3339Nano), Outcome: outcome, Detail: detail})
					return errors.Join(err, writeErr)
				}
				readyCtx, cancelReady := context.WithTimeout(ctx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
				err = step("prepare", func() (string, error) {
					if n, ok := driver.(*narumqDriver); ok {
						return "three nodes ready", n.waitHealth(readyCtx, 0)
					}
					return "three nodes ready", environment.WaitForEMQXClusterReady(readyCtx, options, nodes)
				})
				cancelReady()
				if err != nil {
					return err
				}
				label := fmt.Sprintf("round-%02d", round.Round)
				if err := step("scrape_before", func() (string, error) { return label, driver.scrape(ctx, label+"-before") }); err != nil {
					return err
				}
				markerPath := filepath.Join(artifactDir, "control", label+".json")
				marker := func(state string) error {
					return writeAtomicJSON(markerPath, roundMarker{Round: round.Round, Node: round.Node, State: state, WrittenAt: time.Now().UTC().Format(time.RFC3339Nano)})
				}
				if err := step("marker_draining", func() (string, error) { return "draining", marker("draining") }); err != nil {
					return err
				}
				drainStart := time.Now()
				drainCtx, cancelDrain := context.WithTimeout(ctx, time.Duration(profile.DrainTimeoutMillis)*time.Millisecond)
				err = step("drain", func() (string, error) {
					if err := driver.start(drainCtx, round); err != nil {
						return "", err
					}
					var detail string
					for {
						done, state, err := driver.completed(drainCtx, round)
						detail = state
						if err != nil {
							return detail, err
						}
						if done {
							return detail, nil
						}
						if err := waitForContext(drainCtx, time.Duration(profile.HealthPollMillis)*time.Millisecond); err != nil {
							return detail, fmt.Errorf("drain deadline: %w", err)
						}
					}
				})
				entryPtr.DrainMillis = float64(time.Since(drainStart)) / float64(time.Millisecond)
				cancelDrain()
				if err != nil {
					return err
				}
				if n, ok := driver.(*narumqDriver); ok {
					detail := n.operations[round.Round]
					entryPtr.NaruMQ = &detail
				} else {
					entryPtr.EMQX = &emqxRoundDetail{FinalState: "prohibiting"}
				}
				if target == "narumq" {
					if err := step("will_settle", func() (string, error) {
						return "", waitForContext(ctx, time.Duration(profile.WillSettleMillis)*time.Millisecond)
					}); err != nil {
						return err
					}
				}
				if err := step("scrape_after", func() (string, error) { return label, driver.scrape(ctx, label+"-after") }); err != nil {
					return err
				}
				if n, ok := driver.(*narumqDriver); ok {
					delta, err := willOutcomeDelta(n.snapshots[label+"-before"], n.snapshots[label+"-after"])
					if err != nil {
						return err
					}
					entryPtr.WillOutcomeDelta = delta
					for reason, count := range delta {
						summary.WillOutcomeTotals[reason] += count
					}
					if delta["unexpected_supported_client"] != 0 || delta["unplanned_failure"] != 0 {
						return fmt.Errorf("round %d has invalid Will outcomes: %v", round.Round, delta)
					}
				}
				if err := step("marker_restarting", func() (string, error) { return "restarting", marker("restarting") }); err != nil {
					return err
				}
				restartStart := time.Now()
				restartCtx, cancelRestart := context.WithTimeout(ctx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
				err = step("restart", func() (string, error) { return "same container and volume", driver.restart(restartCtx, round) })
				if err == nil {
					err = step("ready", func() (string, error) { return "ready", driver.ready(restartCtx, round) })
				}
				entryPtr.RestartReadyMillis = float64(time.Since(restartStart)) / float64(time.Millisecond)
				cancelRestart()
				if err != nil {
					return err
				}
				if err := step("marker_ready", func() (string, error) { return "ready", marker("ready") }); err != nil {
					return err
				}
				if err := step("inter_round_settle", func() (string, error) {
					return "", waitForContext(ctx, time.Duration(profile.InterRoundSettleMillis)*time.Millisecond)
				}); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

type drainResponse struct {
	Data struct {
		OperationID string `json:"operationId"`
		State       string `json:"state"`
		Phase       string `json:"phase"`
		StartedAt   string `json:"startedAt"`
		SafeToStop  bool   `json:"safeToStop"`
		Failure     *struct {
			Code           string  `json:"code"`
			BlockingReason *string `json:"blockingReason"`
		} `json:"failure"`
	} `json:"data"`
}

func drainCompletion(data []byte) (drainResponse, bool, error) {
	var response drainResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return response, false, err
	}
	if response.Data.Phase == "FAILED" || response.Data.Phase == "CANCELLED" {
		detail := response.Data.Phase
		if response.Data.Failure != nil {
			detail += " " + response.Data.Failure.Code
			if response.Data.Failure.BlockingReason != nil {
				detail += " " + *response.Data.Failure.BlockingReason
			}
		}
		return response, false, errors.New(detail)
	}
	return response, response.Data.State == "SAFE_TO_STOP" && response.Data.Phase == "COMPLETED" && response.Data.SafeToStop, nil
}

type narumqDriver struct {
	profile                   Profile
	runID, artifactDir, token string
	nodes                     []environment.ClusterNodeResult
	addresses                 []string
	client                    *http.Client
	operations                map[int]narumqRoundDetail
	snapshots                 map[string]map[string]int64
}

func newNaruMQDriver(profile Profile, runID, artifactDir, token string, nodes []environment.ClusterNodeResult) (*narumqDriver, error) {
	d := &narumqDriver{profile: profile, runID: runID, artifactDir: artifactDir, token: token, nodes: nodes, client: &http.Client{Timeout: 5 * time.Second}, operations: make(map[int]narumqRoundDetail), snapshots: make(map[string]map[string]int64)}
	if len(nodes) != 3 {
		return nil, errors.New("rolling controller requires three nodes")
	}
	for _, node := range nodes {
		address := node.PublishedPorts["8080/tcp"]
		if address == "" {
			return nil, fmt.Errorf("node %s has no published management port", node.Name)
		}
		d.addresses = append(d.addresses, "http://"+address)
	}
	return d, nil
}
func (d *narumqDriver) request(ctx context.Context, method, address string, body []byte, key string) ([]byte, int, error) {
	request, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+d.token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := d.client.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return data, response.StatusCode, err
}
func (d *narumqDriver) drainAddress(round roundPlan) string {
	peer := 0
	if round.NodeID == 1 {
		peer = 1
	}
	return d.addresses[peer] + fmt.Sprintf("/api/v1/nodes/%d/drain", round.NodeID)
}
func (d *narumqDriver) start(ctx context.Context, round roundPlan) error {
	body, err := json.Marshal(struct {
		ServerReference string `json:"serverReference"`
		ConnectionRate  int    `json:"connectionRatePerSecond"`
		LBMillis        int64  `json:"loadBalancerPropagationDelayMillis"`
		TimeoutMillis   int64  `json:"timeoutMillis"`
	}{bp07LBReference, d.profile.ConnectionRatePerSecond, d.profile.LBPropagationMillis, d.profile.DrainTimeoutMillis})
	if err != nil {
		return err
	}
	data, status, err := d.request(ctx, http.MethodPost, d.drainAddress(round), body, fmt.Sprintf("bp07-%s-round-%02d", d.runID, round.Round))
	if err != nil {
		return err
	}
	if status != http.StatusAccepted && status != http.StatusOK {
		return fmt.Errorf("start drain HTTP %d: %s", status, data)
	}
	response, _, err := drainCompletion(data)
	if err != nil {
		return err
	}
	d.operations[round.Round] = narumqRoundDetail{OperationID: response.Data.OperationID, ServerStartedAt: response.Data.StartedAt, FinalPhase: response.Data.Phase}
	return nil
}
func (d *narumqDriver) completed(ctx context.Context, round roundPlan) (bool, string, error) {
	data, status, err := d.request(ctx, http.MethodGet, d.drainAddress(round), nil, "")
	if err != nil {
		return false, "", err
	}
	if status != http.StatusOK {
		return false, "", fmt.Errorf("get drain HTTP %d: %s", status, data)
	}
	response, done, err := drainCompletion(data)
	if err == nil {
		d.operations[round.Round] = narumqRoundDetail{OperationID: response.Data.OperationID, ServerStartedAt: response.Data.StartedAt, FinalPhase: response.Data.Phase}
	}
	return done, response.Data.Phase, err
}
func (d *narumqDriver) restart(ctx context.Context, round roundPlan) error {
	return environment.RestartClusterBroker(ctx, d.nodes[round.NodeID-1].BrokerContainer, time.Duration(d.profile.BrokerStopTimeoutSeconds)*time.Second, time.Duration(d.profile.ReadyTimeoutMillis)*time.Millisecond)
}
func (d *narumqDriver) waitHealth(ctx context.Context, id int) error {
	for {
		ready := true
		for i, address := range d.addresses {
			if id != 0 && i != id-1 {
				continue
			}
			_, status, err := d.request(ctx, http.MethodGet, address+"/health/ready", nil, "")
			if err != nil || status != http.StatusOK {
				ready = false
			}
		}
		if ready {
			return nil
		}
		if err := waitForContext(ctx, time.Duration(d.profile.HealthPollMillis)*time.Millisecond); err != nil {
			return err
		}
	}
}
func (d *narumqDriver) ready(ctx context.Context, round roundPlan) error {
	for {
		if err := d.waitHealth(ctx, round.NodeID); err != nil {
			return err
		}
		_, status, err := d.request(ctx, http.MethodGet, d.addresses[round.NodeID-1]+fmt.Sprintf("/api/v1/nodes/%d/drain", round.NodeID), nil, "")
		if err == nil && status == http.StatusNotFound {
			return nil
		}
		if err := waitForContext(ctx, time.Duration(d.profile.HealthPollMillis)*time.Millisecond); err != nil {
			return err
		}
	}
}
func (d *narumqDriver) scrape(ctx context.Context, label string) error {
	totals := make(map[string]int64)
	for _, reason := range willOutcomeReasons {
		totals[reason] = 0
	}
	for i, address := range d.addresses {
		data, status, err := d.request(ctx, http.MethodGet, address+"/metrics", nil, "")
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("metrics node%d HTTP %d", i+1, status)
		}
		path := filepath.Join(d.artifactDir, "metrics", fmt.Sprintf("%s-node%d.prom", label, i+1))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
		values, err := parseWillOutcomes(data)
		if err != nil {
			return fmt.Errorf("node%d metrics: %w", i+1, err)
		}
		for reason, value := range values {
			totals[reason] += value
		}
	}
	d.snapshots[label] = totals
	return nil
}

func parseWillOutcomes(data []byte) (map[string]int64, error) {
	result := make(map[string]int64, len(willOutcomeReasons))
	prefix := "narumq_will_outcomes_total{reason=\""
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		rest := strings.TrimPrefix(line, prefix)
		reason, tail, ok := strings.Cut(rest, "\"}")
		if !ok {
			return nil, errors.New("malformed Will outcome label")
		}
		known := false
		for _, expected := range willOutcomeReasons {
			if reason == expected {
				known = true
				break
			}
		}
		if !known {
			continue
		}
		if _, duplicate := result[reason]; duplicate {
			return nil, fmt.Errorf("duplicate Will outcome %s", reason)
		}
		parts := strings.Fields(tail)
		if len(parts) == 0 {
			return nil, errors.New("missing Will outcome value")
		}
		value, err := strconv.ParseFloat(parts[0], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value >= float64(math.MaxInt64) || math.Trunc(value) != value {
			return nil, fmt.Errorf("invalid Will counter %q", parts[0])
		}
		result[reason] = int64(value)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, reason := range willOutcomeReasons {
		if _, ok := result[reason]; !ok {
			return nil, fmt.Errorf("missing Will outcome %s", reason)
		}
	}
	return result, nil
}
func willOutcomeDelta(before, after map[string]int64) (map[string]int64, error) {
	result := make(map[string]int64, len(willOutcomeReasons))
	for _, reason := range willOutcomeReasons {
		b, bok := before[reason]
		a, aok := after[reason]
		if !bok || !aok {
			return nil, fmt.Errorf("missing Will delta input %s", reason)
		}
		if a < b {
			return nil, fmt.Errorf("Will counter reset during drain: %s", reason)
		}
		result[reason] = a - b
	}
	return result, nil
}

const emqxEvacuationStatusExpression = "case emqx_node_rebalance_evacuation:status() of {enabled, #{state := S}} -> S; disabled -> disabled end."

type emqxDriver struct {
	profile     Profile
	artifactDir string
	nodes       []environment.ClusterNodeResult
	options     environment.ClusterTargetOptions
	statusPoll  int
}

func (d *emqxDriver) command(ctx context.Context, round roundPlan, step string, args ...string) (string, int, error) {
	argv := make([]string, 0, 3+len(args))
	argv = append(argv, "exec", d.nodes[round.NodeID-1].BrokerContainer, "emqx")
	argv = append(argv, args...)
	data, err := exec.CommandContext(ctx, "docker", argv...).CombinedOutput()
	exitCode := 0
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && ctx.Err() == nil && exitErr.ExitCode() >= 0 {
		exitCode = exitErr.ExitCode()
		err = nil
	}
	path := filepath.Join(d.artifactDir, "emqx", fmt.Sprintf("round-%02d-%s.txt", round.Round, step))
	if mkdirErr := os.MkdirAll(filepath.Dir(path), 0o755); mkdirErr != nil {
		return string(data), exitCode, errors.Join(err, mkdirErr)
	}
	return strings.TrimSpace(string(data)), exitCode, errors.Join(err, os.WriteFile(path, data, 0o644))
}
func (d *emqxDriver) start(ctx context.Context, round roundPlan) error {
	migrate := make([]string, 0, 2)
	for i := 1; i <= 3; i++ {
		if i != round.NodeID {
			migrate = append(migrate, fmt.Sprintf("emqx@node%d.bp07.test", i))
		}
	}
	output, exitCode, err := d.command(ctx, round, "start", "ctl", "rebalance", "start", "--evacuation", "--wait-health-check", strconv.FormatInt(d.profile.LBPropagationMillis/1000, 10), "--redirect-to", bp07LBReference, "--conn-evict-rate", strconv.Itoa(d.profile.ConnectionRatePerSecond), "--sess-evict-rate", strconv.Itoa(d.profile.ConnectionRatePerSecond), "--wait-takeover", strconv.Itoa(d.profile.EMQXWaitTakeoverSeconds), "--migrate-to", strings.Join(migrate, " "))
	if err != nil {
		return err
	}
	return emqxCLIResult(output, exitCode, "Rebalance(evacuation) started")
}

func emqxCLIResult(output string, exitCode int, success ...string) error {
	// e5.8.8 rebalance callbacks return true, while nodetool accepts only ok.
	if exitCode == 0 || exitCode == 1 {
		for _, expected := range success {
			if strings.TrimSpace(output) == expected {
				return nil
			}
		}
	}
	return fmt.Errorf("evacuation CLI exit %d: %s", exitCode, output)
}

func emqxCompletion(output string) (bool, error) {
	state := strings.TrimSpace(output)
	switch state {
	case "prohibiting":
		return true, nil
	case "waiting_health_check", "evicting_conns", "waiting_takeover", "evicting_sessions":
		return false, nil
	case "disabled":
		return false, errors.New("evacuation became disabled before completion")
	default:
		return false, fmt.Errorf("unknown evacuation state %q", state)
	}
}
func (d *emqxDriver) status(ctx context.Context, round roundPlan) (string, error) {
	d.statusPoll++
	output, exitCode, err := d.command(ctx, round, fmt.Sprintf("status-%04d", d.statusPoll), "eval", emqxEvacuationStatusExpression)
	if exitCode != 0 {
		err = errors.Join(err, fmt.Errorf("evacuation status exit %d: %s", exitCode, output))
	}
	return output, err
}
func (d *emqxDriver) completed(ctx context.Context, round roundPlan) (bool, string, error) {
	output, err := d.status(ctx, round)
	if err != nil {
		return false, output, err
	}
	done, err := emqxCompletion(output)
	return done, output, err
}
func (d *emqxDriver) restart(ctx context.Context, round roundPlan) error {
	return environment.RestartClusterBroker(ctx, d.nodes[round.NodeID-1].BrokerContainer, time.Duration(d.profile.BrokerStopTimeoutSeconds)*time.Second, time.Duration(d.profile.ReadyTimeoutMillis)*time.Millisecond)
}
func (d *emqxDriver) ready(ctx context.Context, round roundPlan) error {
	if err := environment.WaitForEMQXClusterReady(ctx, d.options, d.nodes); err != nil {
		return err
	}
	output, exitCode, err := d.command(ctx, round, "stop", "ctl", "rebalance", "stop")
	if err != nil {
		return err
	}
	if err := emqxCLIResult(output, exitCode, "Rebalance(evacuation) stopped", "Rebalance stopped", "Rebalance is already disabled"); err != nil {
		return err
	}
	for {
		state, err := d.status(ctx, round)
		if err != nil {
			return err
		}
		if strings.TrimSpace(state) == "disabled" {
			return nil
		}
		if err := waitForContext(ctx, time.Duration(d.profile.HealthPollMillis)*time.Millisecond); err != nil {
			return err
		}
	}
}
func (d *emqxDriver) scrape(context.Context, string) error { return nil }
