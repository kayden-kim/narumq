package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type backendFlags []string

func (b *backendFlags) String() string         { return strings.Join(*b, ";") }
func (b *backendFlags) Set(value string) error { *b = append(*b, value); return nil }

type helperTrace struct {
	path string
	mu   sync.Mutex
}

func (t *helperTrace) record(event map[string]any) error {
	event["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	file, err := os.OpenFile(t.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
func (t *helperTrace) mustRecord(event map[string]any) {
	if err := t.record(event); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type lbBackend struct {
	address, healthURL string
	ready              bool
}

func runLBCLI(args []string) error {
	flags := flag.NewFlagSet("lb", flag.ContinueOnError)
	listen := flags.String("listen", "", "TCP listen address")
	interval := flags.Int("health-interval-ms", 0, "positive readiness poll interval")
	tracePath := flags.String("trace", "", "JSONL trace output")
	var values backendFlags
	flags.Var(&values, "backend", "MQTT address,health URL in priority order")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *listen == "" || *interval <= 0 || *tracePath == "" || len(values) == 0 {
		return fmt.Errorf("--listen, --backend, --health-interval-ms and --trace are required")
	}
	backends := make([]lbBackend, 0, len(values))
	for _, value := range values {
		address, healthURL, ok := strings.Cut(value, ",")
		if !ok || address == "" || healthURL == "" {
			return fmt.Errorf("invalid backend %q", value)
		}
		backends = append(backends, lbBackend{address: address, healthURL: healthURL})
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	trace := &helperTrace{path: *tracePath}
	var mu sync.Mutex
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		client := &http.Client{Timeout: 500 * time.Millisecond}
		ticker := time.NewTicker(time.Duration(*interval) * time.Millisecond)
		defer ticker.Stop()
		for {
			for index := range backends {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, backends[index].healthURL, nil)
				ready := false
				if err == nil {
					resp, requestErr := client.Do(req)
					if requestErr == nil {
						ready = resp.StatusCode == http.StatusOK
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					}
				}
				mu.Lock()
				changed := backends[index].ready != ready
				backends[index].ready = ready
				address := backends[index].address
				mu.Unlock()
				if changed {
					trace.mustRecord(map[string]any{"event": "health", "backend": address, "ready": ready})
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	go func() { <-ctx.Done(); _ = listener.Close() }()
	defer workers.Wait()
	defer stop()
	connection := 0
	for {
		incoming, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		mu.Lock()
		address := ""
		for _, backend := range backends {
			if backend.ready {
				address = backend.address
				break
			}
		}
		mu.Unlock()
		if address == "" {
			trace.mustRecord(map[string]any{"event": "reject"})
			_ = incoming.Close()
			continue
		}
		outgoing, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err != nil {
			trace.mustRecord(map[string]any{"event": "dial_failed", "backend": address})
			_ = incoming.Close()
			continue
		}
		connection++
		trace.mustRecord(map[string]any{"event": "connect", "backend": address, "connection": connection})
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer incoming.Close()
			defer outgoing.Close()
			finished := make(chan struct{}, 2)
			var copies sync.WaitGroup
			copies.Add(2)
			go func() { defer copies.Done(); _, _ = io.Copy(outgoing, incoming); finished <- struct{}{} }()
			go func() { defer copies.Done(); _, _ = io.Copy(incoming, outgoing); finished <- struct{}{} }()
			select {
			case <-ctx.Done():
			case <-finished:
			}
			_ = incoming.Close()
			_ = outgoing.Close()
			copies.Wait()
		}()
	}
}
