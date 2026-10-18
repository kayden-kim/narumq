package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

type stepRecovery struct {
	rto       time.Duration
	recovered bool
	evaluated bool
}

var killStateRank = map[string]int{killStateKilling: 1, killStateDown: 2, killStateRestoring: 3, killStateRestored: 4}

// pollKill reads the kill marker, remembers newly observed states, and reports the latest state.
func (st *stepRun) pollKill() string {
	state, first := st.kill.poll()
	if first {
		st.markerStates = append(st.markerStates, state)
		st.phase("controller-marker", "observed", state)
	}
	return state
}

// awaitKilling waits for the controller's killing marker, acknowledges it before the
// controller kills, and then waits until the failure injection is observable.
func (st *stepRun) awaitKilling(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		state := st.pollKill()
		if state == killStateFailed {
			st.addInvalid("controller reported failure before the kill: " + st.kill.lastMarker.Error)
			return false
		}
		if killStateRank[state] >= 1 {
			if killStateRank[state] > 1 {
				st.addInvalid("controller advanced past killing before the runner acknowledged it")
				return false
			}
			observedAt := time.Now()
			st.col.observeKilling()
			st.killingAt = st.col.now()
			if err := st.kill.ack(observedAt); err != nil {
				st.addInvalid("controller marker: " + err.Error())
				return false
			}
			break
		}
		if time.Now().After(deadline) {
			st.addInvalid(fmt.Sprintf("controller did not write the killing marker within %s", timeout))
			return false
		}
		if !sleepCtx(st.ctx, markerPollInterval) {
			st.addInvalid("step timed out waiting for the killing marker")
			return false
		}
	}
	// The earliest attributable disconnect is the injection time; the down marker is the fallback.
	var downAt time.Time
	for st.col.injection.Load() < 0 {
		state := st.pollKill()
		switch {
		case state == killStateFailed:
			st.addInvalid("controller reported failure during the kill: " + st.kill.lastMarker.Error)
			return false
		case killStateRank[state] >= 2 && downAt.IsZero():
			downAt = time.Now()
		}
		if !downAt.IsZero() && time.Since(downAt) >= time.Second {
			st.col.setInjection(st.col.rel(downAt))
			break
		}
		if time.Now().After(deadline.Add(time.Duration(st.r.profile.MarkerAckTimeoutMillis) * time.Millisecond)) {
			st.addInvalid("no disconnect or down marker observed after the kill")
			return false
		}
		if !sleepCtx(st.ctx, markerPollInterval) {
			st.addInvalid("step timed out waiting for the failure injection")
			return false
		}
	}
	return true
}

// awaitRecovery waits for every affected client to reconnect and every stream to complete a
// success after the injection (§3.6 RTO), stable for a short window, or the recovery deadline.
func (st *stepRun) awaitRecovery() {
	limit := time.Duration(st.r.profile.RecoveryTimeoutMillis) * time.Millisecond
	injection := st.col.injection.Load()
	deadline := st.col.base.Add(time.Duration(injection) + limit + recoveryStableWindow)
	var stableSince time.Time
	st.phase("recovery", "start", "")
	defer func() {
		st.phase("recovery", "end", fmt.Sprintf("rto=%s recovered=%t", st.recovery.rto, st.recovery.recovered))
	}()
	for {
		st.pollKill()
		rto, ok, _ := st.col.recovery()
		if ok {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= recoveryStableWindow {
				st.recovery = stepRecovery{rto: rto, recovered: true, evaluated: true}
				return
			}
		} else {
			stableSince = time.Time{}
		}
		if time.Now().After(deadline) {
			rto, _, _ := st.col.recovery()
			st.recovery = stepRecovery{rto: max(rto, limit+time.Nanosecond), recovered: false, evaluated: true}
			return
		}
		if !sleepCtx(st.ctx, 25*time.Millisecond) {
			st.recovery = stepRecovery{rto: limit + time.Nanosecond, evaluated: true}
			return
		}
	}
}

func (st *stepRun) awaitRestored(timeout time.Duration) {
	st.phase("restore", "wait", "")
	deadline := time.Now().Add(timeout)
	for {
		state := st.pollKill()
		if state == killStateRestored {
			return
		}
		if state == killStateFailed {
			st.addInvalid("controller reported a failed restore: " + st.kill.lastMarker.Error)
			return
		}
		if time.Now().After(deadline) {
			st.addInvalid(fmt.Sprintf("controller did not report restored within %s (last state %q)", timeout, state))
			return
		}
		if !sleepCtx(st.ctx, markerPollInterval) {
			st.addInvalid("step timed out waiting for the restored marker")
			return
		}
	}
}

// drain waits until every ACKed QoS 1/2 delivery has arrived or the drain time passes.
func (st *stepRun) drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for st.col.outstanding.Load() > 0 && time.Now().Before(deadline) {
		if !sleepCtx(st.ctx, 50*time.Millisecond) {
			return
		}
	}
}

// setup connects and subscribes every client at connectRatePerSecond. Any failure ends setup.
func (st *stepRun) setup() error {
	p := st.r.profile
	total := st.cfg.Cells * cellClients
	st.clients = make([]*sessClient, 0, total)
	for _, plan := range st.plans {
		for _, spec := range plan.clients {
			st.clients = append(st.clients, newSessClient(st.env, spec, plan, spec.cell*cellClients+spec.ordinal))
		}
	}
	timeout := time.Duration(total/p.ConnectRatePerSecond+1)*time.Second + time.Minute
	ctx, cancel := context.WithTimeout(st.ctx, timeout)
	defer cancel()
	interval := time.Second / time.Duration(p.ConnectRatePerSecond)
	begin := time.Now()
	sem := make(chan struct{}, setupConcurrency)
	var group sync.WaitGroup
	var failure atomic.Pointer[error]
	for index, client := range st.clients {
		if !sleepUntil(ctx, begin.Add(time.Duration(index)*interval)) {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-sem }()
			if err := client.start(ctx, st.ctx); err != nil {
				st.col.setupFailures.Add(1)
				failure.CompareAndSwap(nil, &err)
				cancel()
				return
			}
			st.mu.Lock()
			st.started = append(st.started, client)
			st.mu.Unlock()
		}()
	}
	group.Wait()
	if err := failure.Load(); err != nil {
		return *err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("setup timed out after %s: %w", timeout, err)
	}
	st.mu.Lock()
	connected := len(st.started)
	st.mu.Unlock()
	if connected != total {
		return fmt.Errorf("connected %d of %d clients", connected, total)
	}
	return nil
}

// startPublishers launches one absolute-schedule goroutine per publisher stream. Phases are
// spread across all streams of the step so offered load is smooth rather than bursty.
func (st *stepRun) startPublishers(ctx context.Context, start time.Time, interval time.Duration) {
	streams := st.cfg.Cells * cellStreams
	for cell, plan := range st.plans {
		for stream := range cellStreams {
			client := st.clients[cell*cellClients+plan.publisher[stream]]
			offset := interval * time.Duration(cell*cellStreams+stream) / time.Duration(streams)
			st.pubWG.Add(1)
			go func() {
				defer st.pubWG.Done()
				for tick := 0; ; tick++ {
					at := start.Add(offset + time.Duration(tick)*interval)
					if !sleepUntil(ctx, at) {
						return
					}
					client.publish(ctx, at, interval)
				}
			}()
		}
	}
}

// cleanup closes every client and discards persistent sessions with a clean-start connect
// and DISCONNECT so no session outlives the step (client IDs are reused across steps).
func (st *stepRun) cleanup() {
	st.enter(winCleanup)
	if st.pubCancel != nil {
		st.pubCancel()
		st.pubWG.Wait()
	}
	p := st.r.profile
	st.mu.Lock()
	started := append([]*sessClient(nil), st.started...)
	st.mu.Unlock()

	var group sync.WaitGroup
	sem := make(chan struct{}, cleanupConcurrency)
	for _, client := range started {
		sem <- struct{}{}
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-sem }()
			client.close()
		}()
	}
	group.Wait()

	ctx, cancel := context.WithTimeout(st.r.ctx, 5*time.Minute)
	defer cancel()
	interval := time.Second / time.Duration(2*p.ConnectRatePerSecond)
	begin := time.Now()
	var failures atomic.Int64
	var first atomic.Pointer[error]
	index := 0
	for _, plan := range st.plans {
		for _, spec := range plan.clients {
			if !spec.persistent || spec.role != roleSubscriber {
				continue
			}
			sleepUntil(ctx, begin.Add(time.Duration(index)*interval))
			index++
			sem <- struct{}{}
			group.Add(1)
			go func() {
				defer group.Done()
				defer func() { <-sem }()
				if err := st.discardSession(ctx, spec); err != nil {
					failures.Add(1)
					first.CompareAndSwap(nil, &err)
				}
			}()
		}
	}
	group.Wait()
	if count := failures.Load(); count > 0 {
		st.addInvalid(fmt.Sprintf("cleanup: %d of %d persistent sessions could not be discarded (first: %v)", count, index, *first.Load()))
	}
	st.phase("cleanup", "end", fmt.Sprintf("persistentSessionsCleaned=%d failures=%d", index, failures.Load()))
}

func (st *stepRun) discardSession(ctx context.Context, spec clientSpec) error {
	options := st.env.connectOptions(spec, true)
	options.SessionExpiryInterval = 0
	rng := rand.New(rand.NewPCG(st.r.profile.Seed, uint64(spec.cell*cellClients+spec.ordinal)))
	var lastErr error
	for attempt := range 3 {
		if attempt > 0 && !sleepCtx(ctx, time.Duration(attempt)*500*time.Millisecond) {
			break
		}
		client, _, err := st.env.endpoints.connect(ctx, rng, spec.tls, options)
		if err != nil {
			lastErr = err
			continue
		}
		err = client.Disconnect(time.Second)
		_ = client.Close()
		return err
	}
	return errors.Join(lastErr, ctx.Err())
}

func (st *stepRun) spanMillis(w windowID) float64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	span := st.spans[w]
	if span.start.IsZero() {
		return 0
	}
	end := span.end
	if end.IsZero() {
		end = time.Now()
	}
	return float64(end.Sub(span.start)) / float64(time.Millisecond)
}
