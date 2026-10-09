package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"narumq/internal/adapters/auditlog"
	portsaudit "narumq/internal/application/ports/audit"
	"net"
	"net/http"
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
				"-allow-insecure-anonymous-mqtt",
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
		"-allow-insecure-anonymous-mqtt",
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
			"-allow-insecure-anonymous-mqtt",
			"-runtime-metrics-output", output,
			"-runtime-metrics-interval", "5ms",
		}, logger)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if data, err := os.ReadFile(output); err == nil && len(data) > 0 {
			first, _, complete := bytes.Cut(data, []byte{'\n'})
			var initial struct {
				CollectedAt time.Time `json:"collectedAt"`
			}
			// Wait for a complete initial record and a later clock tick. The
			// sampler intentionally suppresses equal timestamps on Windows.
			if err := json.Unmarshal(first, &initial); complete && err == nil && !initial.CollectedAt.IsZero() && time.Now().After(initial.CollectedAt) {
				break
			}
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
func TestRunWithContextDefaultsAndFlagErrors(t *testing.T) {
	if err := runWithContext(nil, nil, nil); err != nil {
		t.Fatalf("runWithContext(nil defaults) error = %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := runWithContext(context.Background(), []string{"-unknown"}, logger); err == nil {
		t.Fatal("runWithContext accepted an unknown flag")
	}
}

func TestClusterConfigFlagIsExclusive(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "empty path", args: []string{"-cluster-config", ""}, want: "-cluster-config requires a non-empty file path"},
		{name: "other runtime flags", args: []string{"-cluster-config", "cluster.json", "-tcp-listen", "127.0.0.1:1883"}, want: "-cluster-config cannot be combined with -tcp-listen"},
		{name: "peer-only profile", args: []string{"-cluster-config", "cluster.json", "-raft-peer-config", "peer.json"}, want: "-cluster-config cannot be combined with -raft-peer-config"},
		{name: "positional argument", args: []string{"-cluster-config", "cluster.json", "unexpected"}, want: "-cluster-config does not accept positional arguments"},
		{name: "restore requires config", args: []string{"-cluster-restore", "backup.nmqbak"}, want: "-cluster-restore requires -cluster-config"},
		{name: "restore empty archive path", args: []string{"-cluster-config", "cluster.json", "-cluster-restore", ""}, want: "-cluster-restore requires a non-empty file path"},
		{name: "restore runtime flag conflict", args: []string{"-cluster-config", "cluster.json", "-cluster-restore", "backup.nmqbak", "-tcp-listen", "127.0.0.1:1883"}, want: "-cluster-config cannot be combined with -tcp-listen"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := runWithContext(context.Background(), testCase.args, logger)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("runWithContext() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestRun_Defaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(nil, logger); err != nil {
		t.Fatalf("run(nil) error = %v", err)
	}
}

func TestRunWithContextRejectsConstructionBounds(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "delivery queue", args: []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", "-delivery-queue-capacity", "0"}, want: "create delivery queue"},
		{name: "retained message capacity", args: []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", "-retained-message-capacity", "0"}, want: "retained-message-capacity"},
		{name: "retained byte capacity", args: []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", "-retained-byte-capacity", "0"}, want: "retained-byte-capacity"},
		{name: "decoder", args: []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", "-max-packet-size", "0"}, want: "create MQTT decoder"},
		{name: "server config", args: []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", "-control-queue-capacity", "0"}, want: "create TCP server"},
		{name: "metrics without listener", args: []string{"-runtime-metrics-output", "out.jsonl", "-runtime-metrics-interval", "1s"}, want: "required when runtime metrics collection is enabled"},
		{name: "invalid loopback IP", args: []string{"-tcp-listen", "192.168.1.1:1883", "-allow-insecure-anonymous-mqtt"}, want: "must use literal loopback IP"},
		{name: "invalid tcp address", args: []string{"-tcp-listen", "invalid_no_port", "-allow-insecure-anonymous-mqtt"}, want: "invalid -tcp-listen"},
		{name: "unauthenticated management listener", args: []string{"-http-listen", "0.0.0.0:8080"}, want: "unauthenticated management listener"},
		{name: "negative interval", args: []string{"-runtime-metrics-output", "out.jsonl", "-runtime-metrics-interval", "-1s"}, want: "greater than zero"},
		{name: "unmatched metrics flags output only", args: []string{"-runtime-metrics-output", "out.jsonl"}, want: "must be provided together"},
		{name: "unmatched metrics flags interval only", args: []string{"-runtime-metrics-interval", "1s"}, want: "must be provided together"},
		{name: "negative TCP connection maximum", args: []string{"-tcp-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt", "-tcp-max-connections", "-1"}, want: "-tcp-max-connections must be between 0 and 1000000"},
		{name: "excess TLS connection rate", args: []string{"-tls-listen", "127.0.0.1:0", "-security-config", "unused.json", "-tls-connection-rate", "1000001"}, want: "-tls-connection-rate must be between 0 and 1000000"},
		{name: "TCP limits require TCP listener", args: []string{"-tls-listen", "127.0.0.1:0", "-security-config", "unused.json", "-tcp-max-connections", "1"}, want: "-tcp-max-connections and -tcp-connection-rate require -tcp-listen"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := runWithContext(context.Background(), testCase.args, logger)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("runWithContext() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestRunWithContextCanceledListenerStopsCleanly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := runWithContext(ctx, []string{
		"-tcp-listen", "127.0.0.1:0",
		"-allow-insecure-anonymous-mqtt",
	}, logger); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled listener run error = %v", err)
	}
}
func TestRunWithContextMQTTSecurityMode(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "listener without security mode",
			args: []string{"-tcp-listen", "127.0.0.1:0"},
			want: "-security-config is required when an MQTT listener is enabled",
		},
		{
			name: "security config and insecure exception",
			args: []string{
				"-tcp-listen", "127.0.0.1:0",
				"-security-config", filepath.Join(t.TempDir(), "unused.json"),
				"-allow-insecure-anonymous-mqtt",
			},
			want: "-security-config and -allow-insecure-anonymous-mqtt cannot be used together",
		},
		{
			name: "TLS with insecure exception",
			args: []string{
				"-tls-listen", "127.0.0.1:0",
				"-allow-insecure-anonymous-mqtt",
			},
			want: "-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen",
		},
		{
			name: "insecure exception without MQTT listener",
			args: []string{"-allow-insecure-anonymous-mqtt"},
			want: "-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runWithContext(context.Background(), test.args, logger)
			if err == nil || err.Error() != test.want {
				t.Fatalf("runWithContext() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestMainNoListenerPath(t *testing.T) {
	originalArgs := os.Args
	defer func() { os.Args = originalArgs }()
	os.Args = []string{"narumq"}
	main()
}

func TestRunReportsListenerConstructionFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	address := listener.Addr().String()
	err = runWithContext(context.Background(), []string{
		"-tcp-listen", address,
		"-allow-insecure-anonymous-mqtt",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("runWithContext() error = %v, want listener failure", err)
	}
}

func TestRunWithContextTLSFlags(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tempDir := t.TempDir()
	cfgFile := filepath.Join(tempDir, "valid-sec.json")
	if err := os.WriteFile(cfgFile, []byte(validSecurityJSON), 0600); err != nil {
		t.Fatal(err)
	}
	mtlsCfgFile := filepath.Join(tempDir, "valid-mtls-sec.json")
	if err := os.WriteFile(mtlsCfgFile, []byte(validMTLSSecurityJSON), 0600); err != nil {
		t.Fatal(err)
	}

	t.Run("request requires mTLS identity bindings", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{
			"-security-config", cfgFile,
			"-tls-listen", "127.0.0.1:0",
			"-tls-client-auth", "request",
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "requires at least one mTLS identity binding") {
			t.Fatalf("expected mTLS binding requirement error, got %v", err)
		}
	})

	t.Run("none rejects mTLS identity bindings", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{
			"-security-config", mtlsCfgFile,
			"-tls-listen", "127.0.0.1:0",
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "cannot be used with mTLS identity bindings") {
			t.Fatalf("expected mTLS client-auth conflict, got %v", err)
		}
	})

	t.Run("request requires JWT fallback for certificate-less clients", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{
			"-security-config", mtlsCfgFile,
			"-tls-listen", "127.0.0.1:0",
			"-tls-client-auth", "request",
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "requires JWT configuration") {
			t.Fatalf("expected JWT fallback requirement error, got %v", err)
		}
	})

	t.Run("missing cert with tls-listen", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{
			"-security-config", cfgFile,
			"-tls-listen", "127.0.0.1:0",
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "configure TLS") {
			t.Fatalf("expected configure TLS error, got %v", err)
		}
	})

	t.Run("invalid tls address", func(t *testing.T) {
		certPath := filepath.Join(tempDir, "dummy.crt")
		keyPath := filepath.Join(tempDir, "dummy.key")
		_ = os.WriteFile(certPath, []byte("dummy"), 0600)
		_ = os.WriteFile(keyPath, []byte("dummy"), 0600)
		err := runWithContext(context.Background(), []string{
			"-security-config", cfgFile,
			"-tls-listen", "127.0.0.1:0",
			"-tls-cert", certPath,
			"-tls-key", keyPath,
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "configure TLS") {
			t.Fatalf("expected configure TLS error for dummy cert, got %v", err)
		}
	})
}

// Traceability: SEC-02, SEC-04, TEN-01
func TestRunWithContextSecurityConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tempDir := t.TempDir()

	t.Run("missing file", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{
			"-tcp-listen", "127.0.0.1:0",
			"-security-config", filepath.Join(tempDir, "nonexistent.json"),
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "read security config file") {
			t.Fatalf("expected read security config file error, got %v", err)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		badFile := filepath.Join(tempDir, "bad.json")
		_ = os.WriteFile(badFile, []byte("{invalid"), 0600)
		err := runWithContext(context.Background(), []string{
			"-tcp-listen", "127.0.0.1:0",
			"-security-config", badFile,
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "load security config") {
			t.Fatalf("expected load security config error, got %v", err)
		}
	})

	t.Run("valid security config starts and cancels cleanly", func(t *testing.T) {
		cfgFile := filepath.Join(tempDir, "valid-sec.json")
		_ = os.WriteFile(cfgFile, []byte(validSecurityJSON), 0600)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := runWithContext(ctx, []string{
			"-tcp-listen", "127.0.0.1:0",
			"-security-config", cfgFile,
		}, logger)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled security run error = %v", err)
		}
	})
}

func TestRunWithContextServesManagementHTTP(t *testing.T) {
	reserveAddress := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		return address
	}

	mqttAddress := reserveAddress()
	httpAddress := reserveAddress()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		done <- runWithContext(ctx, []string{
			"-tcp-listen", mqttAddress,
			"-allow-insecure-anonymous-mqtt",
			"-http-listen", httpAddress,
		}, logger)
	}()

	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for {
		response, err := client.Get("http://" + httpAddress + "/health/ready")
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("management readiness status = %d", response.StatusCode)
			}
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("management HTTP server did not become ready: %v", err)
		}
		select {
		case err := <-done:
			t.Fatalf("runWithContext stopped before management readiness: %v", err)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	response, err := client.Get("http://" + httpAddress + "/api/v1/cluster")
	if err != nil {
		t.Fatalf("local management API request failed: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("local management API status = %d, want 200", response.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithContext shutdown error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runWithContext did not stop after cancellation")
	}
}

func TestAuditVerifyCommandVerifiesAndLeavesFilesReadOnly(t *testing.T) {
	directory := t.TempDir()
	publisher, err := auditlog.OpenFileAuditPublisher(auditlog.FileAuditPublisherConfig{
		Directory: directory, MaxFileBytes: auditlog.DefaultMaxFileBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = publisher.Close()
		}
	})
	event := portsaudit.AuditEventDTO{
		SchemaVersion: "1.0", EventID: "event-1",
		OccurredAt: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC), RequestID: "request-1",
		Producer: portsaudit.AuditProducerDTO{Component: "BROKER", InstanceID: "test-cluster/1", Version: "1.2.3"},
		Actor:    portsaudit.AuditActorDTO{Subject: "management-bearer-token", Roles: []string{"ClusterAdmin"}},
		Action:   "node.drain.start", Target: portsaudit.AuditTargetDTO{Type: "node", ID: "3"},
		Result: portsaudit.AuditResultAllowed, Reason: "succeeded",
	}
	if err := publisher.Publish(event); err != nil {
		t.Fatal(err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true

	logPath := filepath.Join(directory, "audit.jsonl")
	original, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := verifyAuditDirectory(directory, &output); err != nil {
		t.Fatalf("verify valid audit chain: %v", err)
	}
	if !strings.Contains(output.String(), "files=1 records=1 first_seq=1 last_seq=1") ||
		!strings.Contains(output.String(), "torn_tail=false") {
		t.Fatalf("verification summary = %q", output.String())
	}
	unchanged, err := os.ReadFile(logPath)
	if err != nil || !bytes.Equal(unchanged, original) {
		t.Fatalf("valid verification changed audit file: read error=%v", err)
	}

	file, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(`{"seq":`)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	tornBefore, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := verifyAuditDirectory(directory, &output); err != nil {
		t.Fatalf("verify recoverable active tail: %v", err)
	}
	if !strings.Contains(output.String(), "records=1") || !strings.Contains(output.String(), "torn_tail=true") {
		t.Fatalf("torn-tail summary = %q", output.String())
	}
	tornAfter, err := os.ReadFile(logPath)
	if err != nil || !bytes.Equal(tornAfter, tornBefore) {
		t.Fatalf("read-only verification modified torn tail: read error=%v", err)
	}

	corrupt := bytes.Replace(tornBefore, []byte(`"requestId":"request-1"`), []byte(`"requestId":"request-x"`), 1)
	if bytes.Equal(corrupt, tornBefore) {
		t.Fatal("corruption did not change the event")
	}
	if err := os.WriteFile(logPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	corruptBefore, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	output.Reset()
	err = verifyAuditDirectory(directory, &output)
	if err == nil || !strings.Contains(err.Error(), "audit.jsonl:1") {
		t.Fatalf("corrupt verification error = %v, want file and line", err)
	}
	if output.Len() != 0 {
		t.Fatalf("corrupt verification emitted success summary: %q", output.String())
	}
	corruptAfter, err := os.ReadFile(logPath)
	if err != nil || !bytes.Equal(corruptAfter, corruptBefore) {
		t.Fatalf("read-only verifier changed corrupt file: read error=%v", err)
	}
}

func TestAuditVerifyFlagRequiresExclusiveDirectory(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	directory := t.TempDir()
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "empty path", args: []string{"-audit-verify", ""}, want: "-audit-verify requires a non-empty directory path"},
		{name: "positional argument", args: []string{"-audit-verify", directory, "extra"}, want: "-audit-verify does not accept positional arguments"},
		{name: "cluster mode conflict", args: []string{"-audit-verify", directory, "-cluster-config", "cluster.json"}, want: "-audit-verify cannot be combined with -cluster-config"},
		{name: "runtime mode conflict", args: []string{"-audit-verify", directory, "-http-listen", "127.0.0.1:8080"}, want: "-audit-verify cannot be combined with -http-listen"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := runWithContext(context.Background(), test.args, logger)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("runWithContext() error = %v, want %q", err, test.want)
			}
		})
	}
}
