package environment

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReadStatsParsesDecimalAndBinaryUnits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.jsonl")
	data := `{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"12.5%","MemUsage":"1.5MiB / 2GB","NetIO":"1.5kB / 2MB","BlockIO":"0B / 0B"}}` + "\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	samples, err := ReadStats(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 || samples[0].CPUPercent != 12.5 || samples[0].MemoryUsageBytes != 1572864 || samples[0].MemoryLimitBytes != 2000000000 || samples[0].NetworkRxBytes != 1500 || samples[0].NetworkTxBytes != 2000000 || samples[0].BlockReadBytes != 0 || samples[0].BlockWriteBytes != 0 {
		t.Fatalf("ReadStats() = %#v", samples)
	}
	if _, ok := samples[0].Docker["MemUsage"]; !ok {
		t.Fatal("ReadStats() did not preserve raw Docker map")
	}
}

func TestReadStatsRejectsUnknownNegativeAndMalformedUnits(t *testing.T) {
	cases := []string{
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1%","MemUsage":"1XB / 2GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B"}}`,
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"1%","MemUsage":"-1B / 2GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B"}}`,
		`{"collectedAt":"2026-09-19T00:00:00Z","container":"broker","docker":{"CPUPerc":"NaN%","MemUsage":"1B / 2GiB","NetIO":"0B / 0B","BlockIO":"0B / 0B"}}`,
	}
	for _, line := range cases {
		path := filepath.Join(t.TempDir(), "stats.jsonl")
		if err := os.WriteFile(path, []byte(line+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadStats(path); err == nil || strings.TrimSpace(err.Error()) == "" {
			t.Fatalf("ReadStats() accepted invalid line %q", line)
		}
	}
}

func TestDecodeLiveDockerStatsLineStripsTerminalControlsAndSplitsUpdates(t *testing.T) {
	first := `{"Container":"broker","CPUPerc":"1%","MemUsage":"1MiB / 2MiB","NetIO":"3B / 4B","BlockIO":"5B / 6B"}`
	second := `{"Container":"broker","CPUPerc":"2%","MemUsage":"2MiB / 2MiB","NetIO":"7B / 8B","BlockIO":"9B / 10B"}`
	line := "\x1b[2K" + first + "\r\x1b[2K" + second

	samples, err := decodeLiveDockerStatsLine(line, "broker")
	if err != nil {
		t.Fatalf("decodeLiveDockerStatsLine() error = %v", err)
	}
	if len(samples) != 2 || samples[0].CPUPercent != 1 || samples[1].CPUPercent != 2 ||
		samples[0].Container != "broker" || samples[1].Container != "broker" {
		t.Fatalf("decoded live stats = %#v", samples)
	}
}

func TestParseByteQuantityRejectsOverflow(t *testing.T) {
	if _, err := parseByteQuantity("999999999999999999999GB"); err == nil {
		t.Fatal("parseByteQuantity() accepted int64 overflow")
	}
}

func TestLiveStatsSamplerSplitsCarriageReturnUpdatesAndReapsOnStop(t *testing.T) {
	installFakeDocker(t)
	t.Setenv(fakeDockerLiveStatsModeEnv, "stream")
	const container = "live-stats-broker"
	if err := writeFakeDockerRunning(container, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "broker-stats.jsonl")
	stop, done := startLiveStatsSampler(context.Background(), container, path)
	doneReceived := false
	defer func() {
		if doneReceived {
			return
		}
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("live stats process was not reaped after stop")
		}
	}()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var samples []StatsSample
	for {
		var err error
		samples, err = ReadStats(path)
		if err == nil && len(samples) >= 2 {
			break
		}
		select {
		case err := <-done:
			doneReceived = true
			t.Fatalf("live stats stream stopped before cancellation: %v", err)
		case <-deadline.C:
			t.Fatalf("live stats stream did not publish carriage-return updates: %v", err)
		case <-ticker.C:
		}
	}
	if samples[0].CPUPercent != 1 || samples[1].CPUPercent != 2 {
		t.Fatalf("live stats samples = %#v, want two ordered updates", samples[:2])
	}

	stop()
	select {
	case err := <-done:
		doneReceived = true
		if err != nil {
			t.Fatalf("stop live stats sampler: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live stats sampler did not stop and reap its process")
	}
}

func TestLiveStatsSamplerRejectsUnexpectedCleanExit(t *testing.T) {
	installFakeDocker(t)
	t.Setenv(fakeDockerLiveStatsModeEnv, "exit")
	const container = "live-stats-exit-broker"
	if err := writeFakeDockerRunning(container, true); err != nil {
		t.Fatal(err)
	}
	stop, done := startLiveStatsSampler(context.Background(), container, filepath.Join(t.TempDir(), "broker-stats.jsonl"))
	defer stop()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "ended unexpectedly") {
			t.Fatalf("unexpected clean stream exit error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live stats sampler accepted a stream that did not exit")
	}
}

// TestStatsSamplerRecordsBrokerRestartedBeforeInspect verifies the stats sampler records a broker that restarted before inspection.
// Scenarios: performance-and-verification.md:PRF-SCN-025
func TestStatsSamplerRecordsBrokerRestartedBeforeInspect(t *testing.T) {
	fakeDocker := installFakeDocker(t)
	t.Setenv(fakeDockerStartAfterStoppedStatsEnv, "1")
	const container = "bp07-race-broker"
	if err := writeFakeDockerRunning(container, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "broker-stats.jsonl")
	stop, done := startStatsSampler(context.Background(), container, path, true)
	defer stop()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		samples, err := ReadStats(path)
		if err == nil && len(samples) > 0 {
			if samples[0].Container != container || samples[0].CPUPercent != 1 ||
				samples[0].MemoryUsageBytes != 1<<20 || samples[0].MemoryLimitBytes != 2<<30 {
				t.Fatalf("restarted broker sample = %#v", samples[0])
			}
			break
		}
		select {
		case err := <-done:
			t.Fatalf("sampler failed after restart race: %v", err)
		case <-deadline.C:
			t.Fatalf("restarted broker sample did not arrive: %v", err)
		case <-ticker.C:
		}
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stop sampler after restart race: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("sampler did not stop after restart race")
	}

	calls := readFakeDockerCalls(t, fakeDocker.callsPath)
	wantCalls := []string{
		"stats --no-stream --format {{json .}} " + container,
		"inspect --format {{.State.Running}} " + container,
		"stats --no-stream --format {{json .}} " + container,
	}
	nextCall := 0
	for _, call := range calls {
		if strings.Join(call, " ") == wantCalls[nextCall] {
			nextCall++
			if nextCall == len(wantCalls) {
				break
			}
		}
	}
	if nextCall != len(wantCalls) {
		t.Fatalf("restart race calls = %#v, want stats, inspect running, stats", calls)
	}
}
