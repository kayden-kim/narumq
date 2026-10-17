package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Scenarios: performance-and-verification.md:PRF-SCN-075, performance-and-verification.md:PRF-SCN-071, performance-and-verification.md:PRF-SCN-103
func TestRunCIRequiresBaseAndPropagatesBaseToCoverage(t *testing.T) {
	if err := runCI(context.Background(), t.TempDir(), "", func(context.Context, string, commandSpec) error { return nil }); err == nil || !strings.Contains(err.Error(), "--base") {
		t.Fatalf("runCI() without base error = %v, want required base", err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n\ngo 1.26.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var commands []commandSpec
	var commandsMu sync.Mutex
	baselineCommandsCompleted := 0
	mutationCommandCount := 0
	if err := runCI(context.Background(), root, "HEAD~1", func(_ context.Context, _ string, spec commandSpec) error {
		commandsMu.Lock()
		defer commandsMu.Unlock()
		commands = append(commands, spec)
		isMutation := len(spec.Args) >= 2 && spec.Args[0] == "tool" && spec.Args[1] == "gremlins"
		if isMutation {
			mutationCommandCount++
			return nil
		}
		isRunnerSetup := len(spec.Args) >= 3 && spec.Args[0] == "run" && spec.Args[1] == "./tools/verify" &&
			(spec.Args[2] == "branch-instrument" || spec.Args[2] == "coverage" || (spec.Args[2] == "scenarios" && len(spec.Args) > 3))
		if !isRunnerSetup {
			baselineCommandsCompleted++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if mutationCommandCount != 0 {
		t.Fatalf("CI ran %d mutation commands, want none", mutationCommandCount)
	}
	if baselineCommandsCompleted != 11 {
		t.Fatalf("baseline completed=%d, want 11 checks", baselineCommandsCompleted)
	}
	if len(commands) != 14 {
		t.Fatalf("command count = %d, want 14", len(commands))
	}
	want := []commandSpec{
		{Name: "go", Args: []string{"run", "./tools/verify", "branch-instrument", "--policy", defaultCoveragePolicyPath, "--output", defaultBranchOutputDir}},
		{Name: "go", Args: []string{"vet", "./..."}},
		{Name: "go", Args: []string{"run", "./tools/verify", "traceability"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "scenarios"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "architecture"}},
		{Name: "go", Dir: defaultBranchOutputDir + "/module", StdoutFile: ".artifacts/verification/go-test.json", Args: []string{"test", "-json", "./...", "-parallel=" + strconv.Itoa(runtime.GOMAXPROCS(0)), "-count=1", "-covermode=atomic", "-coverpkg=./cmd/...,./internal/...", "-coverprofile=" + filepath.Join(root, ".artifacts", "verification", "coverage.out")}},
		{Name: "go", Args: []string{"run", "./tools/verify", "scenarios", "--go-test-json", ".artifacts/verification/go-test.json", "--require-executed"}},
		{Name: "go", Args: []string{"test", "-race", "./cmd/...", "./internal/...", "-count=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzDecoder$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzCodecRoundTrip$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/routing", "-run=^$", "-fuzz=^FuzzTopicMatching$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/session", "-run=^$", "-fuzz=^FuzzLifecycleTransitions$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/adapters/tcp", "-run=^$", "-fuzz=^FuzzConnectionInput$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "coverage", "--profile", ".artifacts/verification/coverage.out", "--policy", defaultCoveragePolicyPath, "--report", ".artifacts/verification/coverage-summary.json", "--branch-manifest", defaultBranchManifestPath, "--base", "HEAD~1"}},
	}
	sort.Slice(commands, func(i, j int) bool { return formatCommand(commands[i]) < formatCommand(commands[j]) })
	sort.Slice(want, func(i, j int) bool { return formatCommand(want[i]) < formatCommand(want[j]) })
	if !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %#v, want %#v", commands, want)
	}
	coverageBaseCount := 0
	for _, command := range commands {
		for index, arg := range command.Args {
			if arg != "--base" {
				continue
			}
			coverageBaseCount++
			if index+1 >= len(command.Args) || command.Args[index+1] != "HEAD~1" || command.Args[0] == "tool" {
				t.Fatalf("invalid base argument = %v", command.Args)
			}
		}
	}
	if coverageBaseCount != 1 {
		t.Fatalf("coverage base count = %d, want 1", coverageBaseCount)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-075, performance-and-verification.md:PRF-SCN-103
func TestRunLocalOmitsMutationChecksAndCoverageWithoutBase(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n\ngo 1.26.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var commands []commandSpec
	var commandsMu sync.Mutex
	if err := runLocal(context.Background(), root, func(_ context.Context, _ string, spec commandSpec) error {
		commandsMu.Lock()
		commands = append(commands, spec)
		commandsMu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		if len(command.Args) >= 2 && command.Args[0] == "tool" && command.Args[1] == "gremlins" {
			t.Fatalf("local command included mutation check: %v", command.Args)
		}
		for _, arg := range command.Args {
			if arg == "--base" {
				t.Fatalf("local command received CI coverage base: %v", command)
			}
		}
	}
	scenarioChecks := 0
	for _, command := range commands {
		if reflect.DeepEqual(command, commandSpec{Name: "go", Args: []string{"run", "./tools/verify", "scenarios"}}) {
			scenarioChecks++
		}
	}
	if scenarioChecks != 1 {
		t.Fatalf("local gate ran the scenarios check %d times, want once", scenarioChecks)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-102
func TestRunMutationExecutesAllPackagesWithOneGremlinsWorker(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n\ngo 1.26.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var commands []commandSpec
	var commandsMu sync.Mutex
	if err := runMutation(context.Background(), root, func(_ context.Context, _ string, spec commandSpec) error {
		commandsMu.Lock()
		commands = append(commands, spec)
		commandsMu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 5 {
		t.Fatalf("mutation command count = %d, want 5", len(commands))
	}
	wantPackages := map[string]string{
		"internal/domain/delivery":  ".artifacts/verification/mutation-delivery.json",
		"internal/domain/mqtt":      ".artifacts/verification/mutation-mqtt.json",
		"internal/domain/nodedrain": ".artifacts/verification/mutation-nodedrain.json",
		"internal/domain/security":  ".artifacts/verification/mutation-security.json",
		"internal/domain/session":   ".artifacts/verification/mutation-session.json",
	}
	for _, command := range commands {
		if command.Name != "go" || len(command.Args) != 7 ||
			command.Args[0] != "tool" || command.Args[1] != "gremlins" ||
			command.Args[2] != "unleash" || command.Args[3] != "--config=tests/verification/gremlins.yaml" ||
			command.Args[6] != "--workers=1" {
			t.Fatalf("unexpected mutation command: %#v", command)
		}
		output, ok := wantPackages[command.Args[5]]
		if !ok {
			t.Fatalf("unexpected mutation package %q", command.Args[5])
		}
		if command.Args[4] != "--output="+output {
			t.Fatalf("mutation output for %s = %q, want %q", command.Args[5], command.Args[4], output)
		}
		delete(wantPackages, command.Args[5])
	}
	if len(wantPackages) != 0 {
		t.Fatalf("mutation packages not invoked: %v", wantPackages)
	}
	if _, err := os.Stat(filepath.Join(root, ".artifacts", "verification")); err != nil {
		t.Fatalf("mutation artifact directory missing: %v", err)
	}
}

func TestRunConcurrentCancelsRemainingCommandsOnFailure(t *testing.T) {
	started := make(chan struct{}, 2)
	failure := errors.New("fuzz target failed")
	runner := func(ctx context.Context, _ string, spec commandSpec) error {
		started <- struct{}{}
		if spec.Args[0] == "fail" {
			<-started
			return failure
		}
		<-ctx.Done()
		return ctx.Err()
	}
	err := runConcurrent(context.Background(), ".", []commandSpec{
		{Name: "go", Args: []string{"fail"}},
		{Name: "go", Args: []string{"wait"}},
	}, runner)
	if !errors.Is(err, failure) {
		t.Fatalf("runConcurrent() error = %v, want original failure", err)
	}
}

func TestMutationCommandConcurrencyUsesAvailableCPU(t *testing.T) {
	tests := []struct {
		name            string
		availableCPUs   int
		workersPerJob   int
		testCPUs        int
		workloadCount   int
		wantConcurrency int
	}{
		{name: "sixteen CPUs run five mutation packages", availableCPUs: 16, workersPerJob: 1, testCPUs: 1, workloadCount: 5, wantConcurrency: 5},
		{name: "eight CPUs run four mutation packages", availableCPUs: 8, workersPerJob: 1, testCPUs: 1, workloadCount: 5, wantConcurrency: 4},
		{name: "four CPUs run two mutation packages", availableCPUs: 4, workersPerJob: 1, testCPUs: 1, workloadCount: 5, wantConcurrency: 2},
		{name: "one CPU keeps one package active", availableCPUs: 1, workersPerJob: 1, testCPUs: 1, workloadCount: 5, wantConcurrency: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mutationCommandConcurrency(test.availableCPUs, test.workersPerJob, test.testCPUs, test.workloadCount); got != test.wantConcurrency {
				t.Fatalf("mutationCommandConcurrency(%d, %d, %d, %d) = %d, want %d",
					test.availableCPUs, test.workersPerJob, test.testCPUs, test.workloadCount, got, test.wantConcurrency)
			}
		})
	}
}

func TestCheckGoFormatDoesNotModifySource(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "main.go")
	original := []byte("package main\n\nfunc main(){ }\n")
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkGoFormat(root); err == nil {
		t.Fatal("checkGoFormat() accepted unformatted source")
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(updated, original) {
		t.Fatal("checkGoFormat() modified source")
	}
}

func TestCheckGoFormatAcceptsCRLFWithoutChangingSource(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "main.go")
	original := []byte("package main\r\n\r\nfunc main() {}\r\n")
	if err := os.WriteFile(path, original, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkGoFormat(root); err != nil {
		t.Fatalf("checkGoFormat() rejected formatted CRLF source: %v", err)
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(updated, original) {
		t.Fatal("checkGoFormat() modified CRLF source")
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-113
func TestExecuteCommandWritesStdoutToFileInsteadOfConsole(t *testing.T) {
	root := t.TempDir()

	err := executeCommand(context.Background(), root, commandSpec{Name: "go", Args: []string{"env", "GOOS"}, StdoutFile: "nested/out.txt"})
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(root, "nested", "out.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != runtime.GOOS {
		t.Fatalf("captured stdout = %q, want %q", got, runtime.GOOS)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-113
func TestRunLocalRunsExecutionCheckAfterTestsAndBeforeCoveragePolicy(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n\ngo 1.26.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var order []string
	var orderMu sync.Mutex
	if err := runLocal(context.Background(), root, func(_ context.Context, _ string, spec commandSpec) error {
		orderMu.Lock()
		defer orderMu.Unlock()
		switch {
		case spec.StdoutFile != "":
			order = append(order, "test-json")
		case len(spec.Args) >= 4 && spec.Args[2] == "scenarios" && spec.Args[len(spec.Args)-1] == "--require-executed":
			order = append(order, "execution-check")
		case len(spec.Args) >= 3 && spec.Args[2] == "coverage":
			order = append(order, "coverage-policy")
		default:
			order = append(order, "other")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	position := func(name string) int {
		for index, item := range order {
			if item == name {
				return index
			}
		}
		return -1
	}
	testIndex, executionIndex, coverageIndex := position("test-json"), position("execution-check"), position("coverage-policy")
	if testIndex < 0 || executionIndex < 0 || coverageIndex < 0 {
		t.Fatalf("order = %v, want go test -json, execution check and coverage policy", order)
	}
	if !(executionIndex > testIndex && executionIndex < coverageIndex) {
		t.Fatalf("order = %v, want execution check after the parallel checks and before the coverage policy", order)
	}
	if executionIndex != len(order)-2 {
		t.Fatalf("order = %v, want the execution check right after the parallel checks", order)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-113
func TestRunLocalPrintsFailedTestsFromJSONAndSkipsExecutionCheck(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n\ngo 1.26.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	events := strings.Join([]string{
		`{"Action":"run","Package":"narumq/pkg","Test":"TestBad"}`,
		`{"Action":"output","Package":"narumq/pkg","Test":"TestBad","Output":"=== RUN   TestBad\n"}`,
		`{"Action":"output","Package":"narumq/pkg","Test":"TestBad","Output":"    bad_test.go:5: boom\n"}`,
		`{"Action":"fail","Package":"narumq/pkg","Test":"TestBad"}`,
		`{"Action":"fail","Package":"narumq/pkg"}`,
	}, "\n") + "\n"
	var stderr strings.Builder
	executionChecks := 0
	err := runVerification(context.Background(), root, verificationOptions{Stderr: &stderr}, func(_ context.Context, _ string, spec commandSpec) error {
		if len(spec.Args) > 0 && spec.Args[len(spec.Args)-1] == "--require-executed" {
			executionChecks++
		}
		if spec.StdoutFile != "" {
			if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(spec.StdoutFile)), []byte(events), 0o644); err != nil {
				return err
			}
			return errors.New("exit status 1")
		}
		return nil
	})

	if err == nil || !strings.Contains(err.Error(), "parallel checks") {
		t.Fatalf("runVerification() error = %v, want a parallel-check failure", err)
	}
	for _, want := range []string{"1 failed test(s)", "FAIL narumq/pkg TestBad", "bad_test.go:5: boom"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
	if strings.Contains(stderr.String(), "=== RUN") {
		t.Fatalf("stderr kept RUN noise:\n%s", stderr.String())
	}
	if executionChecks != 0 {
		t.Fatalf("execution check ran %d times after a failed parallel batch, want none", executionChecks)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-113
func TestPrintGoTestFailuresListsLeafFailuresAndBuildFailures(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "go-test.json")
	events := strings.Join([]string{
		`{"Action":"pass","Package":"a","Test":"TestOK"}`,
		`{"Action":"output","Package":"a","Test":"TestParent/child","Output":"    child_test.go:9: wrong\n"}`,
		`{"Action":"fail","Package":"a","Test":"TestParent/child"}`,
		`{"Action":"fail","Package":"a","Test":"TestParent"}`,
		`{"Action":"fail","Package":"a"}`,
		`{"Action":"build-output","ImportPath":"b [b.test]","Output":"b/x.go:3: undefined: y\n"}`,
		`{"Action":"fail","Package":"b"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(events), 0o644); err != nil {
		t.Fatal(err)
	}
	var out strings.Builder

	printGoTestFailures(&out, path)

	text := out.String()
	for _, want := range []string{"1 failed test(s), 1 failed package(s)", "  FAIL a TestParent/child\n", "child_test.go:9: wrong", "  FAIL b (package failed without a failed test)", "undefined: y"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "  FAIL a TestParent\n") {
		t.Fatalf("output listed the parent of a failed subtest:\n%s", text)
	}

	var missing strings.Builder
	printGoTestFailures(&missing, filepath.Join(root, "absent.json"))
	if missing.Len() != 0 {
		t.Fatalf("missing file output = %q, want none", missing.String())
	}
}
