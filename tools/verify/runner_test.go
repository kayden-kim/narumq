package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunCIRejectsMissingBaseBeforeRunningCommands(t *testing.T) {
	called := false
	err := runCI(context.Background(), t.TempDir(), "", func(context.Context, string, commandSpec) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "--base") || called {
		t.Fatalf("missing base: error=%v command executed=%t, want rejection before side effects", err, called)
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

func TestExecuteCommandPreservesCancellationAndExitCode(t *testing.T) {
	const marker = "verify-command-helper"
	if len(os.Args) >= 3 && os.Args[len(os.Args)-2] == marker {
		if os.Args[len(os.Args)-1] == "fail" {
			os.Exit(23)
		}
		time.Sleep(30 * time.Second)
		return
	}
	for _, mode := range []string{"cancel", "fail"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			if mode == "fail" {
				ctx = context.Background()
			}
			err := executeCommand(ctx, t.TempDir(), commandSpec{
				Name: os.Args[0],
				Args: []string{"-test.run=^TestExecuteCommandPreservesCancellationAndExitCode$", "--", marker, mode},
			})
			var failure commandFailure
			if !errors.As(err, &failure) {
				t.Fatalf("command error = %v, want commandFailure", err)
			}
			if mode == "cancel" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("canceled child error = %v, want context deadline identity", err)
				}
			} else if failure.Code != 23 || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("child exit = %v, want real exit 23 without cancellation", failure)
			}
		})
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
