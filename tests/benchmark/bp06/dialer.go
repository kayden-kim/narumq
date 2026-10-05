package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"narumq/tests/benchmark/internal/jsonl"
	"narumq/tests/benchmark/internal/mqttclient"
)

type readinessTransition struct {
	Node   string `json:"node"`
	Ready  bool   `json:"ready"`
	Status int    `json:"status"`
	At     string `json:"at"`
}

type healthObservation struct {
	ready  bool
	status int
}

// readinessDialer chooses ready broker nodes for new MQTT connections.
type readinessDialer struct {
	target     string
	nodes      []string
	healthURLs []string
	token      string
	poll       time.Duration
	healthLog  *jsonl.Writer
	httpClient *http.Client
	ready      []bool
	known      []bool
	status     []int
	nextIndex  int

	mu        sync.Mutex
	started   bool
	closed    bool
	cancel    context.CancelFunc
	done      chan struct{}
	changed   chan struct{}
	configErr error
	fatalErr  error
}

// newReadinessDialer creates a readiness-based client-side load balancer.
func newReadinessDialer(target string, nodes []string, healthURLs []string, token string, poll time.Duration, healthLog *jsonl.Writer) *readinessDialer {
	dialer := &readinessDialer{
		target:     target,
		nodes:      append([]string(nil), nodes...),
		healthURLs: append([]string(nil), healthURLs...),
		token:      token,
		poll:       poll,
		healthLog:  healthLog,
		httpClient: &http.Client{Timeout: 2 * time.Second},
		ready:      make([]bool, len(nodes)),
		known:      make([]bool, len(nodes)),
		status:     make([]int, len(nodes)),
		done:       make(chan struct{}),
		changed:    make(chan struct{}),
	}
	if len(nodes) == 0 {
		dialer.configErr = errors.New("readiness dialer requires at least one node")
	} else if len(nodes) != len(healthURLs) {
		dialer.configErr = errors.New("readiness node and health URL counts differ")
	} else if poll <= 0 {
		dialer.configErr = errors.New("readiness poll interval must be positive")
	}
	return dialer
}

// Start launches readiness polling and performs the first poll immediately.
func (dialer *readinessDialer) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	dialer.mu.Lock()
	if dialer.started || dialer.closed {
		dialer.mu.Unlock()
		return
	}
	dialer.started = true
	if dialer.configErr != nil {
		dialer.fatalErr = dialer.configErr
		dialer.signalLocked()
		close(dialer.done)
		dialer.mu.Unlock()
		return
	}
	pollCtx, cancel := context.WithCancel(ctx)
	dialer.cancel = cancel
	dialer.mu.Unlock()

	go dialer.run(pollCtx)
}

// WaitAllReady waits until every node has returned HTTP 200 or an error occurs.
func (dialer *readinessDialer) WaitAllReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		dialer.mu.Lock()
		if dialer.fatalErr != nil {
			err := dialer.fatalErr
			dialer.mu.Unlock()
			return err
		}
		if dialer.allReadyLocked() {
			dialer.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			dialer.mu.Unlock()
			return err
		}
		if !dialer.started {
			dialer.mu.Unlock()
			return errors.New("readiness dialer has not started")
		}
		if dialer.closed {
			dialer.mu.Unlock()
			return errors.New("readiness dialer is closed before all nodes are ready")
		}
		changed := dialer.changed
		dialer.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Dial connects to the next ready node in global round-robin order.
func (dialer *readinessDialer) Dial(ctx context.Context, options mqttclient.ConnectOptions) (*mqttclient.Client, mqttclient.ConnectTiming, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		dialer.mu.Lock()
		if dialer.fatalErr != nil {
			err := dialer.fatalErr
			dialer.mu.Unlock()
			return nil, mqttclient.ConnectTiming{}, "", err
		}
		if err := ctx.Err(); err != nil {
			dialer.mu.Unlock()
			return nil, mqttclient.ConnectTiming{}, "", err
		}
		if !dialer.started {
			dialer.mu.Unlock()
			return nil, mqttclient.ConnectTiming{}, "", errors.New("readiness dialer has not started")
		}
		if dialer.closed {
			dialer.mu.Unlock()
			return nil, mqttclient.ConnectTiming{}, "", errors.New("readiness dialer is closed")
		}

		index, ok := dialer.nextReadyLocked()
		if ok {
			dialer.nextIndex = (index + 1) % len(dialer.nodes)
			address := dialer.nodes[index]
			dialer.mu.Unlock()

			options.Address = address
			client, timing, err := mqttclient.Connect(ctx, options)
			if err != nil {
				return nil, timing, fmt.Sprintf("node%d", index+1), err
			}
			return client, timing, fmt.Sprintf("node%d", index+1), nil
		}
		changed := dialer.changed
		dialer.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, mqttclient.ConnectTiming{}, "", ctx.Err()
		case <-changed:
		}
	}
}

// Close stops and joins readiness polling without closing established MQTT clients.
func (dialer *readinessDialer) Close() {
	dialer.mu.Lock()
	if !dialer.closed {
		dialer.closed = true
		dialer.signalLocked()
	}
	cancel := dialer.cancel
	started := dialer.started
	done := dialer.done
	dialer.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if started {
		<-done
	}
	dialer.httpClient.CloseIdleConnections()
}

func (dialer *readinessDialer) run(ctx context.Context) {
	defer func() {
		dialer.mu.Lock()
		dialer.closed = true
		dialer.signalLocked()
		dialer.mu.Unlock()
		close(dialer.done)
	}()

	var workers sync.WaitGroup
	for index, healthURL := range dialer.healthURLs {
		workers.Add(1)
		go func(index int, healthURL string) {
			defer workers.Done()
			dialer.pollNodeLoop(ctx, index, healthURL)
		}(index, healthURL)
	}
	workers.Wait()
}

func (dialer *readinessDialer) pollNodeLoop(ctx context.Context, index int, healthURL string) {
	ticker := time.NewTicker(dialer.poll)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		observation := dialer.pollNode(ctx, healthURL)
		if ctx.Err() == nil {
			if err := dialer.updateNode(index, observation); err != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (dialer *readinessDialer) pollNode(ctx context.Context, healthURL string) healthObservation {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return healthObservation{}
	}
	if dialer.target == "narumq" {
		request.Header.Set("Authorization", "Bearer "+dialer.token)
	}
	response, err := dialer.httpClient.Do(request)
	if err != nil {
		return healthObservation{}
	}
	defer response.Body.Close()
	return healthObservation{ready: response.StatusCode == http.StatusOK, status: response.StatusCode}
}

func (dialer *readinessDialer) updateNode(index int, observation healthObservation) error {
	dialer.mu.Lock()
	if dialer.fatalErr != nil || dialer.closed {
		err := dialer.fatalErr
		dialer.mu.Unlock()
		return err
	}
	if dialer.known[index] && dialer.ready[index] == observation.ready && dialer.status[index] == observation.status {
		dialer.mu.Unlock()
		return nil
	}

	dialer.known[index] = true
	dialer.ready[index] = observation.ready
	dialer.status[index] = observation.status
	event := readinessTransition{
		Node:   fmt.Sprintf("node%d", index+1),
		Ready:  observation.ready,
		Status: observation.status,
		At:     time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := dialer.healthLog.Write(event); err != nil {
		dialer.fatalErr = fmt.Errorf("write readiness health log for %s: %w", event.Node, err)
		fatalErr := dialer.fatalErr
		dialer.signalLocked()
		cancel := dialer.cancel
		dialer.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return fatalErr
	}
	dialer.signalLocked()
	dialer.mu.Unlock()
	return nil
}

func (dialer *readinessDialer) allReadyLocked() bool {
	for _, ready := range dialer.ready {
		if !ready {
			return false
		}
	}
	return true
}

func (dialer *readinessDialer) nextReadyLocked() (int, bool) {
	for offset := range dialer.nodes {
		index := (dialer.nextIndex + offset) % len(dialer.nodes)
		if dialer.ready[index] {
			return index, true
		}
	}
	return 0, false
}

func (dialer *readinessDialer) signalLocked() {
	close(dialer.changed)
	dialer.changed = make(chan struct{})
}
