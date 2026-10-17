package rollout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runControllerBounded runs RunController in a goroutine. A regression that lets an invalid
// configuration pass validation reaches manager construction against the loopback kubeconfig set by
// the test. The test then fails on the 10s bound, or on an error-text assertion such as the
// "manager" check, instead of hanging or touching a real cluster.
func runControllerBounded(t *testing.T, config ControllerConfig, audit *reconcilerStubAudit) error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		if audit == nil {
			result <- RunController(config, nil)
			return
		}
		result <- RunController(config, *audit)
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("RunController did not reject the configuration; it went on to build the manager")
		return nil
	}
}

// unreachableKubeconfig points at a loopback port with no listener. It replaces the developer's
// kubeconfig so a regression cannot reach a real cluster.
const unreachableKubeconfig = `apiVersion: v1
kind: Config
clusters:
- name: unreachable
  cluster:
    server: http://127.0.0.1:1
contexts:
- name: unreachable
  context:
    cluster: unreachable
    user: unreachable
current-context: unreachable
users:
- name: unreachable
  user:
    token: unused
`

func TestRunControllerRejectsIncompleteConfigurationBeforeTouchingTheCluster(t *testing.T) {
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte(unreachableKubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	// t.Setenv restores the previous value after the test and forbids t.Parallel.
	t.Setenv("KUBECONFIG", kubeconfig)
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
			var audit *reconcilerStubAudit
			if !tc.nilAudit {
				audit = &reconcilerStubAudit{}
			}
			err := runControllerBounded(t, config, audit)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RunController error = %v, want one mentioning %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "manager") {
				t.Fatalf("configuration error was reported only after manager construction: %v", err)
			}
		})
	}
}
