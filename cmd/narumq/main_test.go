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

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-07
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

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-02, configuration-and-runtime-options.md:CFG-SCN-18
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

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-01
func TestHashPasswordFlagIsExclusive(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "positional argument", args: []string{"-hash-password", "unexpected"}, want: "-hash-password does not accept positional arguments"},
		{name: "runtime flag", args: []string{"-hash-password", "-tcp-listen", "127.0.0.1:1883"}, want: "-hash-password cannot be combined with -tcp-listen"},
		{name: "cluster mode", args: []string{"-hash-password", "-cluster-config", "cluster.json"}, want: "-hash-password cannot be combined with -cluster-config"},
		{name: "audit verify", args: []string{"-hash-password", "-audit-verify", "audit"}, want: "-hash-password cannot be combined with -audit-verify"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := runWithContext(context.Background(), testCase.args, logger)
			if err == nil || err.Error() != testCase.want {
				t.Fatalf("runWithContext() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-05
func TestRun_Defaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(nil, logger); err != nil {
		t.Fatalf("run(nil) error = %v", err)
	}
}

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-08, mqtt-protocol-and-session.md:MQTT-SCN-052
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

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-06, configuration-and-runtime-options.md:CFG-SCN-41, tenancy-and-security.md:SEC-SCN-012
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
			want: "-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen, -ws-listen, or -wss-listen",
		},
		{
			name: "insecure exception without MQTT listener",
			args: []string{"-allow-insecure-anonymous-mqtt"},
			want: "-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen, -ws-listen, or -wss-listen",
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

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-05
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

	t.Run("request requires credential fallback for certificate-less clients", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{
			"-security-config", mtlsCfgFile,
			"-tls-listen", "127.0.0.1:0",
			"-tls-client-auth", "request",
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "requires JWT or password authentication configuration") {
			t.Fatalf("expected credential fallback requirement error, got %v", err)
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
// Scenarios: tenancy-and-security.md:SEC-SCN-012
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

// TestRunWithContextServesManagementHTTP verifies standalone management routes and fixed identifiers.
// Traceability: PRD API-02; docs/specs/configuration-and-runtime-options.md CFG-SCN-39; ADR-0052 §1.6.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-39
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
	getJSON := func(path string, wantStatus int, target any) {
		t.Helper()
		response, err := client.Get("http://" + httpAddress + path)
		if err != nil {
			t.Fatalf("management API request %q failed: %v", path, err)
		}
		defer response.Body.Close()
		if response.StatusCode != wantStatus {
			t.Fatalf("management API %q status = %d, want %d", path, response.StatusCode, wantStatus)
		}
		if target != nil {
			if err := json.NewDecoder(response.Body).Decode(target); err != nil {
				t.Fatalf("decode management API %q response: %v", path, err)
			}
		}
	}

	var clusterResponse struct {
		Data struct {
			ClusterID    string  `json:"clusterId"`
			LeaderNodeID *string `json:"leaderNodeId"`
		} `json:"data"`
	}
	getJSON("/api/v1/cluster", http.StatusOK, &clusterResponse)
	if clusterResponse.Data.ClusterID != "narumq-cluster-1" {
		t.Fatalf("cluster ID = %q, want %q", clusterResponse.Data.ClusterID, "narumq-cluster-1")
	}
	leaderNodeID := "<nil>"
	if clusterResponse.Data.LeaderNodeID != nil {
		leaderNodeID = *clusterResponse.Data.LeaderNodeID
	}
	if leaderNodeID != "narumq-node-1" {
		t.Fatalf("leader node ID = %q, want %q", leaderNodeID, "narumq-node-1")
	}

	var nodeResponse struct {
		Data struct {
			NodeID string `json:"nodeId"`
		} `json:"data"`
	}
	getJSON("/api/v1/nodes/narumq-node-1", http.StatusOK, &nodeResponse)
	if nodeResponse.Data.NodeID != "narumq-node-1" {
		t.Fatalf("node ID = %q, want %q", nodeResponse.Data.NodeID, "narumq-node-1")
	}
	getJSON("/api/v1/nodes/1", http.StatusNotFound, nil)

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

// TestRunWithContextManagementAuthTokenFile verifies that the standalone management token comes only from a file.
// Traceability: PRD SEC-10; docs/specs/configuration-and-runtime-options.md CFG-SCN-19.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-19
func TestRunWithContextManagementAuthTokenFile(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("missing file", func(t *testing.T) {
		missingPath := filepath.Join(t.TempDir(), "missing.token")
		err := runWithContext(context.Background(), []string{
			"-http-listen", "127.0.0.1:0",
			"-http-auth-token-file", missingPath,
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "read -http-auth-token-file") {
			t.Fatalf("runWithContext error = %v, want token file read error", err)
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("runWithContext error = %v, want os.ErrNotExist", err)
		}

		err = runWithContext(context.Background(), []string{
			"-http-auth-token-file", missingPath,
		}, logger)
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("runWithContext without listeners error = %v, want os.ErrNotExist", err)
		}
	})

	t.Run("blank file", func(t *testing.T) {
		tokenPath := filepath.Join(t.TempDir(), "management.token")
		if err := os.WriteFile(tokenPath, []byte(" \n\t"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := runWithContext(context.Background(), []string{
			"-http-listen", "127.0.0.1:0",
			"-http-auth-token-file", tokenPath,
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "-http-auth-token-file is empty") {
			t.Fatalf("runWithContext error = %v, want empty token file error", err)
		}
	})

	t.Run("removed command-line token flag", func(t *testing.T) {
		err := runWithContext(context.Background(), []string{
			"-http-listen", "127.0.0.1:0",
			"-http-auth-token", "token",
		}, logger)
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined: -http-auth-token") {
			t.Fatalf("runWithContext error = %v, want undefined token flag error", err)
		}
	})

	t.Run("file token protects api", func(t *testing.T) {
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
		tokenPath := filepath.Join(t.TempDir(), "management.token")
		if err := os.WriteFile(tokenPath, []byte("  secret-token \n"), 0o600); err != nil {
			t.Fatal(err)
		}

		httpAddress := reserveAddress()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- runWithContext(ctx, []string{
				"-http-listen", httpAddress,
				"-http-auth-token-file", tokenPath,
			}, logger)
		}()

		client := &http.Client{Timeout: 200 * time.Millisecond}
		deadline := time.Now().Add(2 * time.Second)
		for {
			response, err := client.Get("http://" + httpAddress + "/health/ready")
			if err == nil {
				response.Body.Close()
				if response.StatusCode != http.StatusOK {
					t.Fatalf("management readiness status = %d, want 200", response.StatusCode)
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
			t.Fatalf("unauthenticated management API request failed: %v", err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unauthenticated management API status = %d, want 401", response.StatusCode)
		}

		request, err := http.NewRequest(http.MethodGet, "http://"+httpAddress+"/api/v1/cluster", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer secret-token")
		response, err = client.Do(request)
		if err != nil {
			t.Fatalf("authenticated management API request failed: %v", err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("authenticated management API status = %d, want 200", response.StatusCode)
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
	})
}

// Scenarios: management-api-and-observability.md:MGT-SCN-014, tenancy-and-security.md:SEC-SCN-050
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

// Scenarios: configuration-and-runtime-options.md:CFG-SCN-04
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

// Traceability: PRD MQTT-10; docs/specs/configuration-and-runtime-options.md §6; docs/contracts/process-configuration-v1.md §1.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-40, configuration-and-runtime-options.md:CFG-SCN-41, configuration-and-runtime-options.md:CFG-SCN-42, configuration-and-runtime-options.md:CFG-SCN-43
func TestRunWithContextWebSocketFlags(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	securityFile := filepath.Join(t.TempDir(), "security.json")
	if err := os.WriteFile(securityFile, []byte(validSecurityJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "plain ws requires security", args: []string{"-ws-listen", "127.0.0.1:0"}, want: "-security-config is required when an MQTT listener is enabled"},
		{name: "wss requires security", args: []string{"-wss-listen", "127.0.0.1:0"}, want: "-security-config is required when an MQTT listener is enabled"},
		{name: "plain ws literal loopback only", args: []string{"-ws-listen", "0.0.0.0:1883", "-security-config", securityFile}, want: "-ws-listen must use literal loopback IP"},
		{name: "anonymous ws is forbidden", args: []string{"-ws-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt"}, want: "-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen, -ws-listen, or -wss-listen"},
		{name: "anonymous wss is forbidden", args: []string{"-wss-listen", "127.0.0.1:0", "-allow-insecure-anonymous-mqtt"}, want: "-allow-insecure-anonymous-mqtt requires a loopback-only -tcp-listen and cannot be used with -tls-listen, -ws-listen, or -wss-listen"},
		{name: "ws max requires ws listener", args: []string{"-ws-max-connections", "5"}, want: "-ws-max-connections and -ws-connection-rate require -ws-listen"},
		{name: "wss rate requires wss listener", args: []string{"-wss-connection-rate", "5"}, want: "-wss-max-connections and -wss-connection-rate require -wss-listen"},
		{name: "ws admission maximum boundary", args: []string{"-ws-listen", "127.0.0.1:0", "-security-config", securityFile, "-ws-max-connections", "1000001"}, want: "-ws-max-connections must be between 0 and 1000000"},
		{name: "wss admission rate boundary", args: []string{"-wss-listen", "127.0.0.1:0", "-security-config", securityFile, "-wss-connection-rate", "-1"}, want: "-wss-connection-rate must be between 0 and 1000000"},
		{name: "wss reuses TLS profile and requires certificate", args: []string{"-wss-listen", "127.0.0.1:0", "-security-config", securityFile}, want: "configure TLS:"},
		{name: "client auth binding applies to wss", args: []string{"-wss-listen", "127.0.0.1:0", "-security-config", securityFile, "-tls-client-auth", "require"}, want: "-tls-client-auth require requires at least one mTLS identity binding"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runWithContext(context.Background(), test.args, logger)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("runWithContext() error = %v, want error containing %q", err, test.want)
			}
		})
	}
}

// Traceability: PRD MQTT-10; CFG-SCN-47; docs/specs/configuration-and-runtime-options.md §6; docs/contracts/process-configuration-v1.md §1.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-47
func TestRunWithContextStartsAndStopsWebSocketListeners(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	directory := t.TempDir()
	securityFile := filepath.Join(directory, "security.json")
	if err := os.WriteFile(securityFile, []byte(validSecurityJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	ca, caKey, _ := newClusterRuntimeTestCA(t)
	certificate, key, _ := issueClusterRuntimeTestCertificate(t, ca, caKey, "mqtt.example.test", 42)
	certificateFile := filepath.Join(directory, "server.pem")
	keyFile := filepath.Join(directory, "server-key.pem")
	if err := os.WriteFile(certificateFile, certificate, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, key, 0o600); err != nil {
		t.Fatal(err)
	}

	wsReservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer wsReservation.Close()
	wssReservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer wssReservation.Close()
	wsAddress, wssAddress := wsReservation.Addr().String(), wssReservation.Addr().String()
	if err := wsReservation.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wssReservation.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runWithContext(ctx, []string{
			"-security-config", securityFile,
			"-ws-listen", wsAddress,
			"-wss-listen", wssAddress,
			"-tls-cert", certificateFile,
			"-tls-key", keyFile,
		}, logger)
	}()
	finished := false
	defer func() {
		cancel()
		if finished {
			return
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("runWithContext did not stop after cancellation")
		}
	}()

	waitForListener := func(address string) {
		deadline := time.Now().Add(2 * time.Second)
		for {
			connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
			if err == nil {
				_ = connection.Close()
				return
			}
			select {
			case runErr := <-done:
				finished = true
				t.Fatalf("runWithContext returned before listener %s was ready: %v", address, runErr)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("listener %s did not accept connections: %v", address, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitForListener(wsAddress)
	waitForListener(wssAddress)

	cancel()
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatalf("runWithContext() error after cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runWithContext did not stop after cancellation")
	}
}
