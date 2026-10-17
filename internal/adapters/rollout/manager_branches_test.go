package rollout

import (
	"strings"
	"testing"
	"time"
)

// runControllerBounded runs RunController in a goroutine. A regression that lets an invalid
// configuration reach manager construction or Start fails the test instead of hanging it.
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

func TestRunControllerRejectsIncompleteConfigurationBeforeTouchingTheCluster(t *testing.T) {
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
