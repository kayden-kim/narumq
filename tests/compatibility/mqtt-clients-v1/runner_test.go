package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

type recordingCommandRunner struct {
	calls [][]string
}

func (runner *recordingCommandRunner) Run(_ context.Context, _ string, name string, args ...string) ([]byte, error) {
	call := append([]string{name}, args...)
	runner.calls = append(runner.calls, call)
	return []byte("ok"), nil
}

func TestCleanupRemovesContainersInReverseRegistrationOrder(t *testing.T) {
	recorder := &recordingCommandRunner{}
	run := &compatibilityRun{options: runOptions{Runner: recorder}}
	run.registerContainer("first")
	run.registerContainer("second")
	run.cleanup(context.Background())
	if len(recorder.calls) != 2 {
		t.Fatalf("cleanup calls = %d, want 2", len(recorder.calls))
	}
	if got := strings.Join(recorder.calls[0], " "); !strings.Contains(got, "rm --force second") {
		t.Fatalf("first cleanup call = %q", got)
	}
	if got := strings.Join(recorder.calls[1], " "); !strings.Contains(got, "rm --force first") {
		t.Fatalf("second cleanup call = %q", got)
	}
}

func TestPathWithinRejectsSiblingEscape(t *testing.T) {
	if !pathWithin("artifacts/run", "artifacts/run/case/result.json") {
		t.Fatal("pathWithin rejected descendant")
	}
	if pathWithin("artifacts/run", "artifacts/run-other/result.json") {
		t.Fatal("pathWithin accepted sibling escape")
	}
}

func TestRunClientMountsProxyTraceReadOnly(t *testing.T) {
	recorder := &recordingCommandRunner{}
	run := &compatibilityRun{
		options: runOptions{RunID: "test", ProfilePath: "profile.json", Runner: recorder},
		images:  map[string]string{"go": "go-image"},
	}
	expected := ExpectedCase{CaseID: "go-mqtt311-qos1-retransmit", Proxy: true}
	role := ExpectedResult{SDK: "go", Role: "client"}
	if _, err := run.runClient(context.Background(), expected, role, "broker", "127.0.0.1:1884", "artifacts/run/case/client", false, ""); err != nil {
		t.Fatal(err)
	}
	if len(recorder.calls) != 1 {
		t.Fatalf("runClient calls = %d, want 1", len(recorder.calls))
	}
	call := strings.Join(recorder.calls[0], " ")
	if !strings.Contains(call, "destination=/proxy-output,readonly") {
		t.Fatalf("client command omitted read-only proxy mount: %q", call)
	}
	if !strings.Contains(call, "--rm") {
		t.Fatalf("client command omitted non-detached cleanup: %q", call)
	}
}

type crossCaseCommandRunner struct {
	t         *testing.T
	expected  ExpectedCase
	readiness *crossReadiness
	calls     [][]string
}

func (runner *crossCaseCommandRunner) Run(_ context.Context, _ string, name string, args ...string) ([]byte, error) {
	call := append([]string{name}, args...)
	runner.calls = append(runner.calls, call)
	if name != "docker" {
		return nil, nil
	}
	role := commandFlag(args, "--role")
	if role == "subscriber" && commandHas(args, "--detach") {
		output := commandMountSource(args, "/output")
		if output == "" {
			runner.t.Fatal("subscriber command omitted output mount")
		}
		signal := crossReadiness{
			SchemaVersion: crossReadinessSchemaVersion,
			CaseID:        runner.expected.CaseID,
			SDK:           runner.expected.ExpectedResults[1].SDK,
			Role:          "subscriber",
			Protocol:      runner.expected.Protocol,
			Ready:         true,
		}
		if runner.readiness != nil {
			signal = *runner.readiness
		}
		if err := writeJSON(filepath.Join(output, "ready.json"), signal); err != nil {
			runner.t.Fatal(err)
		}
	}
	if role == "publisher" {
		input := commandMountSource(args, "/input/readiness.json")
		if input == "" {
			runner.t.Fatal("publisher command omitted readiness input mount")
		}
		if _, err := readCrossReadiness(input, runner.expected, runner.expected.ExpectedResults[1]); err != nil {
			runner.t.Fatalf("publisher received invalid readiness input: %v", err)
		}
	}
	return []byte("ok"), nil
}

func TestRunCrossCaseValidatesAndMountsReadinessBeforePublisher(t *testing.T) {
	expected := ExpectedCase{
		CaseID:   "go-to-python-mqtt311-qos1",
		Protocol: "mqtt311",
		ExpectedResults: []ExpectedResult{
			{SDK: "python", Role: "publisher"},
			{SDK: "go", Role: "subscriber"},
		},
	}
	runner := &crossCaseCommandRunner{t: t, expected: expected}
	run := &compatibilityRun{
		options: runOptions{RunID: "test", ProfilePath: "profile.json", Runner: runner},
		images:  map[string]string{"go": "go-image", "python": "python-image"},
	}
	if err := run.runCrossCase(context.Background(), expected, "broker", "", "127.0.0.1:1883", filepath.Join(t.TempDir(), "case")); err != nil {
		t.Fatal(err)
	}
	subscriberRun, publisherRun := -1, -1
	for index, call := range runner.calls {
		if commandFlag(call[1:], "--role") == "subscriber" && commandHas(call[1:], "--detach") {
			subscriberRun = index
		}
		if commandFlag(call[1:], "--role") == "publisher" {
			publisherRun = index
		}
	}

	if subscriberRun < 0 || publisherRun < 0 || publisherRun <= subscriberRun {
		t.Fatalf("cross command order subscriber=%d publisher=%d calls=%v", subscriberRun, publisherRun, runner.calls)
	}
}
func TestRunCrossCaseRejectsInvalidReadinessBeforePublisher(t *testing.T) {
	expected := ExpectedCase{
		CaseID:   "go-to-python-mqtt311-qos1",
		Protocol: "mqtt311",
		ExpectedResults: []ExpectedResult{
			{SDK: "python", Role: "publisher"},
			{SDK: "go", Role: "subscriber"},
		},
	}
	tests := []struct {
		name   string
		mutate func(*crossReadiness)
	}{
		{
			name: "mismatched case",
			mutate: func(readiness *crossReadiness) {
				readiness.CaseID = "python-to-go-mqtt311-qos1"
			},
		},
		{
			name: "not ready",
			mutate: func(readiness *crossReadiness) {
				readiness.Ready = false
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			readiness := &crossReadiness{
				SchemaVersion: crossReadinessSchemaVersion,
				CaseID:        expected.CaseID,
				SDK:           expected.ExpectedResults[1].SDK,
				Role:          "subscriber",
				Protocol:      expected.Protocol,
				Ready:         true,
			}
			test.mutate(readiness)
			runner := &crossCaseCommandRunner{t: t, expected: expected, readiness: readiness}
			run := &compatibilityRun{
				options: runOptions{RunID: "test", ProfilePath: "profile.json", Runner: runner},
				images:  map[string]string{"go": "go-image", "python": "python-image"},
			}
			if err := run.runCrossCase(context.Background(), expected, "broker", "", "127.0.0.1:1883", filepath.Join(t.TempDir(), "case")); err == nil {
				t.Fatal("runCrossCase accepted invalid subscriber readiness")
			}
			for _, call := range runner.calls {
				if commandFlag(call[1:], "--role") == "publisher" {
					t.Fatalf("publisher command ran after invalid readiness: %v", runner.calls)
				}
			}
		})
	}
}

func commandFlag(args []string, name string) string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == name {
			return args[index+1]
		}
	}
	return ""
}

func commandHas(args []string, value string) bool {
	for _, arg := range args {
		if arg == value {
			return true
		}
	}
	return false
}

func commandMountSource(args []string, destination string) string {
	for _, arg := range args {
		parts := strings.Split(arg, ",")
		source := ""
		dest := ""
		for _, part := range parts {
			switch {
			case strings.HasPrefix(part, "source="):
				source = strings.TrimPrefix(part, "source=")
			case strings.HasPrefix(part, "destination="):
				dest = strings.TrimPrefix(part, "destination=")
			}
		}
		if dest == destination {
			return source
		}
	}
	return ""
}
