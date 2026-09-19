package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateLoopbackListenAddress(t *testing.T) {
	valid := []string{"127.0.0.1:0", "127.255.255.254:1883", "[::1]:0"}
	for _, address := range valid {
		t.Run("valid/"+address, func(t *testing.T) {
			if err := validateLoopbackListenAddress(address); err != nil {
				t.Fatalf("validateLoopbackListenAddress(%q) error = %v", address, err)
			}
		})
	}
	invalid := []string{
		"localhost:1883",
		":1883",
		"0.0.0.0:1883",
		"192.0.2.1:1883",
		"[::]:1883",
		"[::ffff:127.0.0.1]:1883",
		"127.0.0.1",
	}
	for _, address := range invalid {
		t.Run("invalid/"+address, func(t *testing.T) {
			if err := validateLoopbackListenAddress(address); err == nil {
				t.Fatalf("validateLoopbackListenAddress(%q) unexpectedly succeeded", address)
			}
		})
	}
}

func TestRunRejectsRuntimeMetricsFlagCombinations(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	output := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "output only",
			args: []string{"-runtime-metrics-output", output},
		},
		{
			name: "interval only",
			args: []string{"-runtime-metrics-interval", "1s"},
		},
		{
			name: "non-positive interval",
			args: []string{
				"-tcp-listen", "127.0.0.1:0",
				"-runtime-metrics-output", output,
				"-runtime-metrics-interval", "-1s",
			},
		},
		{
			name: "missing listener",
			args: []string{
				"-runtime-metrics-output", output,
				"-runtime-metrics-interval", "1s",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := runWithContext(context.Background(), test.args, logger); err == nil {
				t.Fatal("runWithContext() accepted invalid runtime metrics flags")
			}
		})
	}
}

func TestRunRejectsUnwritableRuntimeMetricsOutput(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	output := filepath.Join(t.TempDir(), "missing", "runtime-metrics.jsonl")
	err := runWithContext(context.Background(), []string{
		"-tcp-listen", "127.0.0.1:0",
		"-runtime-metrics-output", output,
		"-runtime-metrics-interval", "1ms",
	}, logger)
	if err == nil || !strings.Contains(err.Error(), "create runtime metrics output") {
		t.Fatalf("runWithContext() error = %v, want unwritable output failure", err)
	}
}

func TestRunFlushesRuntimeMetricsOnCancellation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	output := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runWithContext(ctx, []string{
			"-tcp-listen", "127.0.0.1:0",
			"-runtime-metrics-output", output,
			"-runtime-metrics-interval", "5ms",
		}, logger)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if info, err := os.Stat(output); err == nil && info.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime metrics output did not receive an initial sample")
		}
		select {
		case err := <-done:
			t.Fatalf("runWithContext() stopped before cancellation: %v", err)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithContext() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runWithContext() did not stop after cancellation")
	}

	file, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	var last map[string]any
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		last = record
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count < 2 {
		t.Fatalf("runtime metrics sample count = %d, want immediate and final samples", count)
	}
	if _, ok := last["collectedAt"].(string); !ok {
		t.Fatalf("last runtime metrics record = %#v, want collectedAt", last)
	}
}
