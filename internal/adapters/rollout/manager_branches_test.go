package rollout

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// useLoopbackKubeconfig points controller-runtime at an unreachable loopback API server so that
// RunController can build a manager without a cluster. The manager never connects before it fails.
func useLoopbackKubeconfig(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	config := `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster: {server: "http://127.0.0.1:1"}
users:
- name: test
  user: {token: test}
contexts:
- name: test
  context: {cluster: test, user: test}
current-context: test
`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", path)
}

// holdProbePort occupies the health-probe port RunController binds, so manager construction fails
// deterministically instead of blocking in manager.Start.
func holdProbePort(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		t.Skipf("health probe port 8081 is not available for this test: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
}

func TestRunControllerRejectsIncompleteConfigurationBeforeTouchingTheCluster(t *testing.T) {
	useLoopbackKubeconfig(t)
	holdProbePort(t)
	valid := ControllerConfig{Namespace: "ns", ProducerID: "producer", ProducerVersion: "1.0.0", Registries: []string{"https://registry.test"}}

	cases := []struct {
		name     string
		mutate   func(*ControllerConfig)
		nilAudit bool
		want     string
	}{
		{"namespace missing", func(c *ControllerConfig) { c.Namespace = "" }, false, "incomplete"},
		{"producer id missing", func(c *ControllerConfig) { c.ProducerID = "" }, false, "incomplete"},
		{"producer version missing", func(c *ControllerConfig) { c.ProducerVersion = "" }, false, "incomplete"},
		{"registry allowlist missing", func(c *ControllerConfig) { c.Registries = nil }, false, "incomplete"},
		{"audit publisher missing", func(*ControllerConfig) {}, true, "incomplete"},
		{"blank registry entry", func(c *ControllerConfig) { c.Registries = []string{"https://registry.test", "  "} }, false, "empty entry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := valid
			config.Registries = append([]string(nil), valid.Registries...)
			tc.mutate(&config)
			audit := reconcilerStubAudit{}
			var err error
			if tc.nilAudit {
				err = RunController(config, nil)
			} else {
				err = RunController(config, audit)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RunController error = %v, want one mentioning %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "manager") {
				t.Fatalf("configuration error was reported only after manager construction: %v", err)
			}
		})
	}
}

func TestRunControllerReportsManagerConstructionFailure(t *testing.T) {
	useLoopbackKubeconfig(t)
	holdProbePort(t)
	config := ControllerConfig{Namespace: "ns", ProducerID: "producer", ProducerVersion: "1.0.0", Registries: []string{" https://registry.test "}}

	err := RunController(config, reconcilerStubAudit{})
	if err == nil {
		t.Fatal("RunController succeeded although the health probe port is occupied")
	}
	if !strings.Contains(err.Error(), "create rollout controller manager") || !strings.Contains(err.Error(), ":8081") {
		t.Fatalf("error = %v, want manager construction failure naming the probe address", err)
	}
}
