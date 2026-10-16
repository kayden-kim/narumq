package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"narumq/tests/benchmark/internal/jsonl"
)

const (
	recoveryStableWindow = time.Second
	leaderLookupTimeout  = 15 * time.Second
	cleanupConcurrency   = 64
	setupConcurrency     = 256
)

type cpuSample struct {
	at      time.Time
	percent float64
}

type resourceRecord struct {
	At         string  `json:"at"`
	Window     string  `json:"window"`
	CPUPercent float64 `json:"generatorCpuPercent,omitempty"`
	CPUKnown   bool    `json:"cpuKnown"`
	RSSBytes   uint64  `json:"rssBytes"`
	Goroutines int     `json:"goroutines"`
}

type phaseEvent struct {
	Phase     string `json:"phase"`
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
	Detail    string `json:"detail,omitempty"`
}

// stepRun executes one step-run (§3.9 단계 절차): setup, warmup, measureN, failure,
// recovery, measureN1, restore, cleanup.
type stepRun struct {
	r     *runner
	cfg   stepConfig
	dir   string
	ctx   context.Context
	col   *collector
	env   *stepEnv
	plans []*cellPlan

	mu       sync.Mutex
	started  []*sessClient
	clients  []*sessClient
	spans    [numWindows]struct{ start, end time.Time }
	samples  []cpuSample
	peakRSS  uint64
	invalid  []string
	setupErr string

	phases   *jsonl.Writer
	resource *jsonl.Writer

	pubCancel context.CancelFunc
	pubWG     sync.WaitGroup

	leader       string
	kill         *killPhase
	markerStates []string
	killingAt    int64
	recovery     stepRecovery
}

func (st *stepRun) addInvalid(reason string) {
	st.mu.Lock()
	st.invalid = append(st.invalid, reason)
	st.mu.Unlock()
}

func (st *stepRun) phase(name, event, detail string) {
	_ = st.phases.Write(phaseEvent{Phase: name, Event: event, Timestamp: formatTime(time.Now()), Detail: detail})
}

// enter closes the current window and opens w.
func (st *stepRun) enter(w windowID) {
	now := time.Now()
	st.mu.Lock()
	previous := st.col.window()
	if st.spans[previous].end.IsZero() && !st.spans[previous].start.IsZero() {
		st.spans[previous].end = now
	}
	st.spans[w].start = now
	st.mu.Unlock()
	st.col.setWindow(w)
	st.phase(w.String(), "start", "")
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sleepUntil(ctx context.Context, at time.Time) bool { return sleepCtx(ctx, time.Until(at)) }

// stepTimeoutFor bounds the measured portion of one step-run (cleanup has its own bound).
func stepTimeoutFor(p Profile, cells int) time.Duration {
	paced := time.Duration(cells*cellClients/p.ConnectRatePerSecond+1) * time.Second
	total := paced + 5*time.Minute
	for _, millis := range []int64{p.WarmupMillis, 2 * p.MeasureMillis, p.DrainMillis, 3 * p.ReadyTimeoutMillis, p.ResyncTimeoutMillis, 2 * p.RecoveryTimeoutMillis} {
		total += time.Duration(millis) * time.Millisecond
	}
	return total
}

func (st *stepRun) stepTimeout() time.Duration { return stepTimeoutFor(st.r.profile, st.cfg.Cells) }

// runSampler writes resource.jsonl once per second and keeps the CPU samples for windows.
func (st *stepRun) runSampler(ctx context.Context) {
	cpus := st.r.profile.GeneratorCPUs
	lastCPU, known := processCPUSeconds()
	last := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			record := resourceRecord{At: formatTime(now), Window: st.col.window().String(), RSSBytes: processRSSBytes(), Goroutines: runtime.NumGoroutine()}
			cpu, ok := processCPUSeconds()
			if known && ok {
				record.CPUKnown = true
				record.CPUPercent = (cpu - lastCPU) / now.Sub(last).Seconds() / cpus * 100
				st.mu.Lock()
				st.samples = append(st.samples, cpuSample{at: now, percent: record.CPUPercent})
				st.mu.Unlock()
			}
			lastCPU, known, last = cpu, ok, now
			st.mu.Lock()
			st.peakRSS = max(st.peakRSS, record.RSSBytes)
			st.mu.Unlock()
			_ = st.resource.Write(record)
		}
	}
}

// averageCPU returns the mean generator CPU percent over a window span.
func (st *stepRun) averageCPU(w windowID) (float64, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	span := st.spans[w]
	if span.start.IsZero() {
		return 0, false
	}
	end := span.end
	if end.IsZero() {
		end = time.Now()
	}
	var sum float64
	count := 0
	for _, sample := range st.samples {
		if sample.at.After(span.start) && !sample.at.After(end) {
			sum += sample.percent
			count++
		}
	}
	if count == 0 {
		return 0, false
	}
	return sum / float64(count), true
}

// execute runs the whole step-run and returns its verdict. Only failures to write artifacts
// are returned as errors; every measurement outcome is a verdict.
func (r *runner) executeStep(cfg stepConfig) (stepOutcome, error) {
	if fileExists(filepath.Join(r.controlDir, "finish.json")) {
		return stepOutcome{}, errors.New("controller wrote finish.json; stopping before step " + fmt.Sprint(cfg.Step))
	}
	dir := filepath.Join(r.outDir, "steps", fmt.Sprintf("%02d-r%d", cfg.Step, cfg.Run))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return stepOutcome{}, err
	}
	phases, err := jsonl.Open(filepath.Join(dir, "phases.jsonl"))
	if err != nil {
		return stepOutcome{}, err
	}
	connections, err := jsonl.Open(filepath.Join(dir, "connections.jsonl"))
	if err != nil {
		return stepOutcome{}, errors.Join(err, phases.Close())
	}
	messages, err := jsonl.Open(filepath.Join(dir, "messages-sample.jsonl"))
	if err != nil {
		return stepOutcome{}, errors.Join(err, phases.Close(), connections.Close())
	}
	resource, err := jsonl.Open(filepath.Join(dir, "resource.jsonl"))
	if err != nil {
		return stepOutcome{}, errors.Join(err, phases.Close(), connections.Close(), messages.Close())
	}

	p := r.profile
	plans := make([]*cellPlan, cfg.Cells)
	for cell := range plans {
		plans[cell] = buildCell(p, cell)
	}
	token := r.nonce<<16 | uint32(cfg.Step)<<8 | uint32(cfg.Run)
	col := newCollector(p, token, plans)
	st := &stepRun{r: r, cfg: cfg, dir: dir, col: col, plans: plans, phases: phases, resource: resource, kill: newKillPhase(r.controlDir, cfg.Step, cfg.Run)}
	st.env = &stepEnv{profile: p, col: col, endpoints: r.endpoints, creds: r.creds, events: connections}
	ctx, cancel := context.WithTimeout(r.ctx, st.stepTimeout())
	defer cancel()
	st.ctx = ctx
	started := time.Now()

	samplerCtx, stopSampler := context.WithCancel(ctx)
	samplerDone := make(chan struct{})
	go func() { defer close(samplerDone); st.runSampler(samplerCtx) }()

	st.stages()

	// Gap oracle over the whole step before clients disappear.
	drainRemaining := col.outstanding.Load()
	delivery := col.evaluate(time.Duration(p.LatencyP99LimitMillis)*time.Millisecond, func(record messageRecord) { _ = messages.Write(record) })

	st.cleanup()
	stopSampler()
	<-samplerDone
	summary := st.summarize(started, delivery, drainRemaining)
	_ = st.phases.Write(phaseEvent{Phase: "step", Event: "end", Timestamp: formatTime(time.Now()), Detail: summary.Verdict})
	closeErr := errors.Join(phases.Close(), connections.Close(), messages.Close(), resource.Close())
	if err := writeAtomicJSON(filepath.Join(dir, "step-summary.json"), summary); err != nil {
		return stepOutcome{}, errors.Join(err, closeErr)
	}
	if err := writeAtomicJSON(markerPath(r.controlDir, cfg.Step, cfg.Run, "complete"), completeMarker{
		Step: cfg.Step, Run: cfg.Run, Verdict: summary.Verdict, Reasons: summary.Reasons, WrittenAt: formatTime(time.Now()),
	}); err != nil {
		return stepOutcome{}, errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return stepOutcome{}, closeErr
	}
	return stepOutcome{
		Verdict: summary.Verdict, Reasons: summary.Reasons, Summary: &summary,
		SummaryPath: filepath.ToSlash(filepath.Join("steps", fmt.Sprintf("%02d-r%d", cfg.Step, cfg.Run), "step-summary.json")),
	}, nil
}

// stages runs readiness through restore. Any failure records its cause and ends the
// measured portion; the caller always evaluates and cleans up afterwards.
func (st *stepRun) stages() {
	p := st.r.profile
	ready := time.Duration(p.ReadyTimeoutMillis) * time.Millisecond

	st.phase("ready", "start", "")
	if err := st.r.mgmt.waitSettled(st.ctx, ready); err != nil {
		st.addInvalid("readiness/resync before setup: " + err.Error())
		st.phase("ready", "end", err.Error())
		return
	}
	active, err := st.r.mgmt.ensureFeatures(st.ctx, requiredClusterFeatures, ready)
	if err != nil {
		st.addInvalid("cluster feature activation before setup: " + err.Error())
		st.phase("ready", "end", err.Error())
		return
	}
	st.r.features = active
	st.phase("ready", "end", "")

	st.enter(winSetup)
	if err := st.setup(); err != nil {
		st.setupErr = err.Error()
		st.phase("setup", "end", err.Error())
		return
	}
	st.phase("setup", "end", "")

	interval := time.Second / time.Duration(st.cfg.Rate)
	pubCtx, pubCancel := context.WithCancel(st.ctx)
	st.pubCancel = pubCancel
	start := time.Now().Add(200 * time.Millisecond)
	st.startPublishers(pubCtx, start, interval)
	st.enter(winWarmup)
	if !sleepCtx(st.ctx, time.Duration(p.WarmupMillis)*time.Millisecond) {
		st.addInvalid("step timed out during warmup")
		return
	}
	st.enter(winMeasureN)
	if !sleepCtx(st.ctx, time.Duration(p.MeasureMillis)*time.Millisecond) {
		st.addInvalid("step timed out during measureN")
		return
	}
	st.enter(winFailure)

	leader, err := st.r.mgmt.leaderWithRetry(st.ctx, leaderLookupTimeout)
	if err != nil {
		st.addInvalid("controller: " + err.Error())
		return
	}
	st.leader = leader
	if err := writeAtomicJSON(markerPath(st.r.controlDir, st.cfg.Step, st.cfg.Run, "ready"), readyMarker{
		Step: st.cfg.Step, Run: st.cfg.Run, Cells: st.cfg.Cells, Rate: st.cfg.Rate, Phase: "measureN-done",
		LeaderNodeID: leader, WrittenAt: formatTime(time.Now()),
	}); err != nil {
		st.addInvalid("controller marker: " + err.Error())
		return
	}
	if !st.awaitKilling(ready) {
		return
	}
	st.awaitRecovery()

	st.enter(winMeasureN1)
	sleepCtx(st.ctx, time.Duration(p.MeasureMillis)*time.Millisecond)
	st.enter(winRestore)
	st.pubCancel()
	st.pubWG.Wait()
	if err := writeAtomicJSON(markerPath(st.r.controlDir, st.cfg.Step, st.cfg.Run, "n1-done"), n1DoneMarker{
		Step: st.cfg.Step, Run: st.cfg.Run, Phase: "measureN1-done", WrittenAt: formatTime(time.Now()),
	}); err != nil {
		st.addInvalid("controller marker: " + err.Error())
		return
	}
	st.awaitRestored(ready + time.Duration(p.ResyncTimeoutMillis)*time.Millisecond + 30*time.Second)
	st.phase("drain", "start", "")
	st.drain(time.Duration(p.DrainMillis) * time.Millisecond)
	st.phase("drain", "end", "")
}
