package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"narumq/tests/benchmark/internal/environment"
	"narumq/tests/benchmark/internal/jsonl"
)

type roundPlan struct {
	Round  int
	Node   string
	NodeID int
}
type failureDriver interface {
	prepare(context.Context) error
	rejoinReady(context.Context, roundPlan) error
	resync(context.Context, roundPlan) (resyncDetail, error)
}
type resyncDetail struct {
	NaruMQ *narumqRoundDetail
	EMQX   *emqxRoundDetail
}
type narumqRoundDetail struct {
	LeaderNode            int    `json:"leaderNode"`
	WatermarkAppliedIndex uint64 `json:"watermarkAppliedIndex"`
	TargetMatchIndex      uint64 `json:"targetMatchIndex"`
}
type emqxRoundDetail struct{}
type failureRoundSummary struct {
	Round             int                `json:"round"`
	Node              string             `json:"node"`
	KillMillis        float64            `json:"killMillis"`
	DownMillis        float64            `json:"downMillis"`
	RejoinReadyMillis float64            `json:"rejoinReadyMillis"`
	ResyncMillis      float64            `json:"resyncMillis"`
	KillStartedAt     string             `json:"killStartedAt"`
	SettleFinishedAt  string             `json:"settleFinishedAt"`
	RestartStartedAt  string             `json:"restartStartedAt"`
	RestartFinishedAt string             `json:"restartFinishedAt"`
	NaruMQ            *narumqRoundDetail `json:"narumq,omitempty"`
	EMQX              *emqxRoundDetail   `json:"emqx,omitempty"`
}
type timeWindow struct {
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
}
type failureSummary struct {
	SchemaVersion  int                   `json:"schemaVersion"`
	Target         string                `json:"target"`
	Valid          bool                  `json:"valid"`
	FailureReasons []string              `json:"failureReasons"`
	Steady         timeWindow            `json:"steady"`
	Rounds         []failureRoundSummary `json:"rounds"`
}
type failureEvent struct {
	Round      int    `json:"round"`
	Node       string `json:"node"`
	Step       string `json:"step"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
	Outcome    string `json:"outcome"`
	Detail     string `json:"detail"`
}

func newFailureController(target string, profile Profile, runID, artifactDir string, fixture *bp06Fixture, options environment.ClusterTargetOptions) func(context.Context, []environment.ClusterNodeResult) error {
	return func(ctx context.Context, nodes []environment.ClusterNodeResult) (runErr error) {
		summary := failureSummary{SchemaVersion: 1, Target: target, Valid: true, FailureReasons: []string{}, Rounds: []failureRoundSummary{}}
		defer func() {
			if runErr != nil {
				summary.Valid = false
				summary.FailureReasons = append(summary.FailureReasons, runErr.Error())
			}
			if err := writeAtomicJSON(filepath.Join(artifactDir, "failure-summary.json"), summary); err != nil {
				runErr = errors.Join(runErr, err)
			}
			status, reason := "success", ""
			if runErr != nil {
				status, reason = "failed", runErr.Error()
			}
			runErr = errors.Join(runErr, writeAtomicJSON(filepath.Join(artifactDir, "control", "failure-complete.json"), failureCompletion{
				Status: status, Reason: reason, WrittenAt: formatTime(time.Now()),
			}))
		}()
		log, err := jsonl.Open(filepath.Join(artifactDir, "failure.jsonl"))
		if err != nil {
			return err
		}
		defer func() { runErr = errors.Join(runErr, log.Close()) }()
		if len(nodes) != 3 {
			return fmt.Errorf("failure controller requires three nodes, got %d", len(nodes))
		}
		var driver failureDriver
		switch target {
		case "narumq":
			if fixture == nil {
				return errors.New("NaruMQ failure controller requires a fixture")
			}
			d, err := newNaruMQDriver(profile, fixture.ManagementToken, nodes)
			if err != nil {
				return err
			}
			driver = d
		case "emqx":
			driver = &emqxDriver{profile: profile, nodes: nodes, options: options}
		default:
			return fmt.Errorf("unsupported failure target %q", target)
		}
		for {
			var ready struct {
				Connections int `json:"connections"`
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
		prepareCtx, cancelPrepare := context.WithTimeout(ctx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
		err = driver.prepare(prepareCtx)
		cancelPrepare()
		if err != nil {
			return err
		}
		steadyStart := time.Now()
		if err := waitForContext(ctx, time.Duration(profile.SteadyMillis)*time.Millisecond); err != nil {
			summary.Steady = timeWindow{StartedAt: formatTime(steadyStart), FinishedAt: formatTime(time.Now())}
			return err
		}
		steadyEnd := time.Now()
		summary.Steady = timeWindow{StartedAt: formatTime(steadyStart), FinishedAt: formatTime(steadyEnd)}
		if err := log.Write(failureEvent{Round: 0, Step: "steady", StartedAt: formatTime(steadyStart), FinishedAt: formatTime(steadyEnd), Outcome: "success"}); err != nil {
			return err
		}

		for cycle := 0; cycle < profile.Cycles; cycle++ {
			for _, id := range profile.KillOrder {
				round := roundPlan{Round: len(summary.Rounds) + 1, Node: fmt.Sprintf("node%d", id), NodeID: id}
				summary.Rounds = append(summary.Rounds, failureRoundSummary{Round: round.Round, Node: round.Node})
				entry := &summary.Rounds[len(summary.Rounds)-1]
				step := func(name string, fn func() error) error {
					started := time.Now()
					stepErr := fn()
					finished := time.Now()
					outcome, detail := "success", ""
					if stepErr != nil {
						outcome, detail = "failed", stepErr.Error()
					}
					writeErr := log.Write(failureEvent{Round: round.Round, Node: round.Node, Step: name, StartedAt: formatTime(started), FinishedAt: formatTime(finished), Outcome: outcome, Detail: detail})
					return errors.Join(stepErr, writeErr)
				}
				prepCtx, cancelPrep := context.WithTimeout(ctx, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
				err := step("prepare", func() error { return driver.prepare(prepCtx) })
				cancelPrep()
				if err != nil {
					return err
				}
				markerPath := filepath.Join(artifactDir, "control", fmt.Sprintf("round-%02d.json", round.Round))
				marker := func(state string) error { return writeFailureMarker(markerPath, round, state) }
				if err := step("marker_killing", func() error { return marker("killing") }); err != nil {
					return err
				}
				ackDeadline := time.Now().Add(time.Duration(profile.MarkerAckTimeoutMillis) * time.Millisecond)
				if err := step("marker_ack", func() error { return waitMarkerAck(ctx, artifactDir, round, ackDeadline) }); err != nil {
					return err
				}
				killStart := time.Now()
				entry.KillStartedAt = formatTime(killStart)
				if err := step("kill", func() error {
					return environment.KillClusterBroker(ctx, nodes[id-1].BrokerContainer, time.Duration(profile.KillTimeoutMillis)*time.Millisecond)
				}); err != nil {
					return err
				}
				entry.KillMillis = float64(time.Since(killStart)) / float64(time.Millisecond)
				downStart := time.Now()
				if err := step("marker_down", func() error { return marker("down") }); err != nil {
					return err
				}
				if err := step("down", func() error { return waitForContext(ctx, time.Duration(profile.DownMillis)*time.Millisecond) }); err != nil {
					entry.DownMillis = float64(time.Since(downStart)) / float64(time.Millisecond)
					return err
				}
				entry.DownMillis = float64(time.Since(downStart)) / float64(time.Millisecond)
				if err := step("marker_restarting", func() error { return marker("restarting") }); err != nil {
					return err
				}
				restartStart := time.Now()
				entry.RestartStartedAt = formatTime(restartStart)
				if err := step("restart", func() error {
					return environment.StartClusterBroker(ctx, nodes[id-1].BrokerContainer, time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond)
				}); err != nil {
					return err
				}
				entry.RestartFinishedAt = formatTime(time.Now())
				readyCtx, cancelReady := context.WithDeadline(ctx, restartStart.Add(time.Duration(profile.ReadyTimeoutMillis)*time.Millisecond))
				if err := step("rejoin_ready", func() error { return driver.rejoinReady(readyCtx, round) }); err != nil {
					cancelReady()
					return err
				}
				cancelReady()
				entry.RejoinReadyMillis = float64(time.Since(restartStart)) / float64(time.Millisecond)
				resyncCtx, cancelResync := context.WithDeadline(ctx, restartStart.Add(time.Duration(profile.ResyncTimeoutMillis)*time.Millisecond))
				var detail resyncDetail
				if err := step("resync", func() error { var e error; detail, e = driver.resync(resyncCtx, round); return e }); err != nil {
					cancelResync()
					return err
				}
				cancelResync()
				entry.ResyncMillis = float64(time.Since(restartStart)) / float64(time.Millisecond)
				entry.NaruMQ, entry.EMQX = detail.NaruMQ, detail.EMQX
				if err := step("marker_ready", func() error { return marker("ready") }); err != nil {
					return err
				}
				if err := step("settle", func() error {
					return waitForContext(ctx, time.Duration(profile.InterRoundSettleMillis)*time.Millisecond)
				}); err != nil {
					entry.SettleFinishedAt = formatTime(time.Now())
					return err
				}
				entry.SettleFinishedAt = formatTime(time.Now())
			}
		}
		return nil
	}
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }
func writeFailureMarker(path string, round roundPlan, state string) error {
	return writeAtomicJSON(path, roundMarker{
		Round: round.Round, Node: round.Node, State: state, WrittenAt: formatTime(time.Now()),
	})
}

func waitMarkerAck(ctx context.Context, artifactDir string, round roundPlan, deadline time.Time) error {
	path := filepath.Join(artifactDir, "control", fmt.Sprintf("round-%02d-ack.json", round.Round))
	for {
		var ack struct {
			Round int    `json:"round"`
			State string `json:"state"`
		}
		if readControlJSON(path, &ack) == nil && ack.Round == round.Round && ack.State == "killing" {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("runner did not acknowledge round %02d killing marker", round.Round)
		}
		poll := 50 * time.Millisecond
		if poll > remaining {
			poll = remaining
		}
		if err := waitForContext(ctx, poll); err != nil {
			return err
		}
	}
}

type narumqDriver struct {
	profile   Profile
	token     string
	addresses []string
	client    *http.Client
}

func newNaruMQDriver(profile Profile, token string, nodes []environment.ClusterNodeResult) (*narumqDriver, error) {
	if len(nodes) != 3 {
		return nil, errors.New("failure controller requires three nodes")
	}
	d := &narumqDriver{profile: profile, token: token, client: &http.Client{Timeout: 5 * time.Second}}
	for _, node := range nodes {
		address := node.PublishedPorts["8080/tcp"]
		if address == "" {
			return nil, fmt.Errorf("node %s has no published management port", node.Name)
		}
		d.addresses = append(d.addresses, "http://"+address)
	}
	return d, nil
}

type narumqNode struct {
	NodeID            string `json:"nodeId"`
	Role              string `json:"role"`
	LastAppliedIndex  uint64 `json:"lastAppliedIndex"`
	ReplicationHealth string `json:"replicationHealth"`
}
type narumqNodePage struct {
	Data []narumqNode `json:"data"`
	Page struct {
		HasMore bool `json:"hasMore"`
	} `json:"page"`
}

func (d *narumqDriver) request(ctx context.Context, address string) (narumqNodePage, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address+"/api/v1/nodes", nil)
	if err != nil {
		return narumqNodePage{}, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+d.token)
	resp, err := d.client.Do(req)
	if err != nil {
		return narumqNodePage{}, 0, err
	}
	defer resp.Body.Close()
	var page narumqNodePage
	if resp.StatusCode == http.StatusOK {
		err = json.NewDecoder(resp.Body).Decode(&page)
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return page, resp.StatusCode, err
}
func (d *narumqDriver) waitHealth(ctx context.Context, only int) error {
	for {
		allReady := true
		for i, address := range d.addresses {
			if only != 0 && i != only-1 {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, address+"/health/ready", nil)
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+d.token)
			resp, err := d.client.Do(req)
			if err != nil {
				allReady = false
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				allReady = false
			}
		}
		if allReady {
			return nil
		}
		if err := waitForContext(ctx, time.Duration(d.profile.HealthPollMillis)*time.Millisecond); err != nil {
			return err
		}
	}
}
func (d *narumqDriver) prepare(ctx context.Context) error { return d.waitHealth(ctx, 0) }
func (d *narumqDriver) rejoinReady(ctx context.Context, round roundPlan) error {
	return d.waitHealth(ctx, round.NodeID)
}
func nodeNumber(id string) int { n, _ := strconv.Atoi(id); return n }
func findLeader(page narumqNodePage) (narumqNode, bool) {
	for _, node := range page.Data {
		if node.Role == "LEADER" {
			return node, true
		}
	}
	return narumqNode{}, false
}
func findNode(page narumqNodePage, id int) (narumqNode, bool) {
	for _, n := range page.Data {
		if nodeNumber(n.NodeID) == id {
			return n, true
		}
	}
	return narumqNode{}, false
}
func (d *narumqDriver) resync(ctx context.Context, round roundPlan) (resyncDetail, error) {
	survivor := 0
	if round.NodeID == 1 {
		survivor = 1
	}
	var watermark uint64
	var watermarkSet bool
	for {
		page, status, err := d.request(ctx, d.addresses[survivor])
		if err == nil && status == http.StatusOK && !page.Page.HasMore {
			if observedLeader, ok := findLeader(page); ok {
				leaderID := nodeNumber(observedLeader.NodeID)
				if leaderID >= 1 && leaderID <= len(d.addresses) {
					leaderPage, code, leaderErr := d.request(ctx, d.addresses[leaderID-1])
					if leaderErr == nil && code == http.StatusOK && !leaderPage.Page.HasMore {
						leaderRow, isLeader := findNode(leaderPage, leaderID)
						if isLeader && leaderRow.Role == "LEADER" {
							if !watermarkSet {
								watermark, watermarkSet = leaderRow.LastAppliedIndex, true
							}
							target, found := findNode(leaderPage, round.NodeID)
							reached := found && target.LastAppliedIndex >= watermark
							if reached && (round.NodeID == leaderID || target.ReplicationHealth == "HEALTHY") {
								if err := d.waitHealth(ctx, 0); err != nil {
									return resyncDetail{}, err
								}
								return resyncDetail{NaruMQ: &narumqRoundDetail{
									LeaderNode: leaderID, WatermarkAppliedIndex: watermark,
									TargetMatchIndex: target.LastAppliedIndex,
								}}, nil
							}
						}
					}
				}
			}
		}
		if err := waitForContext(ctx, time.Duration(d.profile.HealthPollMillis)*time.Millisecond); err != nil {
			return resyncDetail{}, err
		}
	}
}

type emqxDriver struct {
	profile Profile
	nodes   []environment.ClusterNodeResult
	options environment.ClusterTargetOptions
}

func (d *emqxDriver) prepare(ctx context.Context) error {
	return environment.WaitForEMQXClusterReady(ctx, d.options, d.nodes)
}
func (d *emqxDriver) rejoinReady(ctx context.Context, round roundPlan) error {
	for {
		cmd := exec.CommandContext(ctx, "docker", "exec", d.nodes[round.NodeID-1].BrokerContainer, "emqx", "eval", "emqx:is_running().")
		out, err := cmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "true" {
			return nil
		}
		if err := waitForContext(ctx, time.Duration(d.profile.HealthPollMillis)*time.Millisecond); err != nil {
			return err
		}
	}
}
func (d *emqxDriver) resync(ctx context.Context, _ roundPlan) (resyncDetail, error) {
	if err := environment.WaitForEMQXClusterReady(ctx, d.options, d.nodes); err != nil {
		return resyncDetail{}, err
	}
	return resyncDetail{EMQX: &emqxRoundDetail{}}, nil
}
