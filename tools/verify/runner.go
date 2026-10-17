package main

import (
	"context"
	"errors"
	"fmt"
	"go/format"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

type commandSpec struct {
	Name string
	Args []string
	Dir  string
	// StdoutFile, when set, receives the command's stdout instead of the console. It is relative to the
	// repository root unless absolute. Stderr still streams to the console.
	StdoutFile string
}

type commandRunner func(context.Context, string, commandSpec) error

type commandFailure struct {
	Spec commandSpec
	Code int
	Err  error
}

func (failure commandFailure) Error() string {
	return fmt.Sprintf("command %s exited with code %d: %v", formatCommand(failure.Spec), failure.Code, failure.Err)
}

func (failure commandFailure) Unwrap() error {
	return failure.Err
}

type verificationOptions struct {
	CI   bool
	Base string
	// Stderr receives the failed-test summary. It defaults to os.Stderr.
	Stderr io.Writer
}

// Scenarios: performance-and-verification.md:PRF-SCN-070, performance-and-verification.md:PRF-SCN-089
func runLocal(ctx context.Context, root string, runner commandRunner) error {
	return runVerification(ctx, root, verificationOptions{}, runner)
}

// Scenarios: performance-and-verification.md:PRF-SCN-071
func runCI(ctx context.Context, root, base string, runner commandRunner) error {
	if strings.TrimSpace(base) == "" {
		return fmt.Errorf("ci requires --base <git-ref>")
	}
	return runVerification(ctx, root, verificationOptions{CI: true, Base: base}, runner)
}

// Scenarios: performance-and-verification.md:PRF-SCN-075, performance-and-verification.md:PRF-SCN-103
func runVerification(ctx context.Context, root string, options verificationOptions, runner commandRunner) error {
	if err := ensureRepositoryRoot(root); err != nil {
		return err
	}
	if err := checkGoFormat(root); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, ".artifacts", "verification"), 0o755); err != nil {
		return fmt.Errorf("create verification artifact directory: %w", err)
	}
	if err := runner(ctx, root, commandSpec{Name: "go", Args: []string{"run", "./tools/verify", "branch-instrument", "--policy", defaultCoveragePolicyPath, "--output", defaultBranchOutputDir}}); err != nil {
		return fmt.Errorf("verification stopped during branch instrumentation: %w", err)
	}
	fuzzCommands := []commandSpec{
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzDecoder$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzCodecRoundTrip$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/routing", "-run=^$", "-fuzz=^FuzzTopicMatching$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/session", "-run=^$", "-fuzz=^FuzzLifecycleTransitions$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/adapters/tcp", "-run=^$", "-fuzz=^FuzzConnectionInput$", "-fuzztime=10000x", "-parallel=1"}},
	}
	preflightCommands := []commandSpec{
		{Name: "go", Args: []string{"vet", "./..."}},
		{Name: "go", Args: []string{"run", "./tools/verify", "traceability"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "scenarios"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "architecture"}},
	}
	testParallelLimit := 8
	if options.CI {
		testParallelLimit = runtime.GOMAXPROCS(0)
	}
	coverageArgs := []string{"test", "-json", "./...", fmt.Sprintf("-parallel=%d", testParallelLimit), "-count=1", "-timeout=3h", "-covermode=atomic", "-coverpkg=./cmd/...,./internal/...", "-coverprofile=" + filepath.Join(root, ".artifacts", "verification", "coverage.out")}
	parallelCommands := append([]commandSpec{}, preflightCommands...)
	parallelCommands = append(parallelCommands,
		commandSpec{Name: "go", Dir: defaultBranchOutputDir + "/module", Args: coverageArgs, StdoutFile: defaultGoTestJSONPath},
		commandSpec{Name: "go", Args: []string{"test", "-race", "./cmd/...", "./internal/...", "-count=1"}},
	)
	parallelCommands = append(parallelCommands, fuzzCommands...)
	if err := runConcurrent(ctx, root, parallelCommands, runner); err != nil {
		stderr := options.Stderr
		if stderr == nil {
			stderr = os.Stderr
		}
		printGoTestFailures(stderr, filepath.Join(root, filepath.FromSlash(defaultGoTestJSONPath)))
		return fmt.Errorf("verification stopped during parallel checks: %w", err)
	}
	executionCommand := commandSpec{
		Name: "go",
		Args: []string{"run", "./tools/verify", "scenarios", "--go-test-json", defaultGoTestJSONPath, "--require-executed"},
	}
	if err := runner(ctx, root, executionCommand); err != nil {
		return fmt.Errorf("verification stopped after %s: %w", formatCommand(executionCommand), err)
	}
	coverageCommand := commandSpec{
		Name: "go",
		Args: []string{"run", "./tools/verify", "coverage", "--profile", ".artifacts/verification/coverage.out", "--policy", defaultCoveragePolicyPath, "--report", ".artifacts/verification/coverage-summary.json", "--branch-manifest", defaultBranchManifestPath},
	}
	if options.CI {
		coverageCommand.Args = append(coverageCommand.Args, "--base", options.Base)
	}
	if err := runner(ctx, root, coverageCommand); err != nil {
		return fmt.Errorf("verification stopped after %s: %w", formatCommand(coverageCommand), err)
	}
	return nil
}

// Scenarios: performance-and-verification.md:PRF-SCN-102
func runMutation(ctx context.Context, root string, runner commandRunner) error {
	if err := ensureRepositoryRoot(root); err != nil {
		return err
	}
	if err := checkGoFormat(root); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, ".artifacts", "verification"), 0o755); err != nil {
		return fmt.Errorf("create verification artifact directory: %w", err)
	}
	commands := mutationCommands()
	concurrency := mutationCommandConcurrency(runtime.GOMAXPROCS(0), 1, 1, len(commands))
	for start := 0; start < len(commands); start += concurrency {
		end := min(start+concurrency, len(commands))
		if err := runConcurrent(ctx, root, commands[start:end], runner); err != nil {
			return fmt.Errorf("mutation verification stopped during package checks: %w", err)
		}
	}
	if err := verifyMutationReports(root, os.Stdout); err != nil {
		return fmt.Errorf("mutation verification failed report judgment: %w", err)
	}
	return nil
}

func mutationCommands() []commandSpec {
	return []commandSpec{
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-delivery.json", "internal/domain/delivery", "--workers=1"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-mqtt.json", "internal/domain/mqtt", "--workers=1"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-nodedrain.json", "internal/domain/nodedrain", "--workers=1"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-security.json", "internal/domain/security", "--workers=1"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-session.json", "internal/domain/session", "--workers=1"}},
	}
}

func mutationCommandConcurrency(availableCPUs, workersPerCommand, testCPUs, workloadCount int) int {
	if availableCPUs < 1 || workersPerCommand < 1 || testCPUs < 1 || workloadCount < 1 {
		return 1
	}
	commandLimit := availableCPUs / (workersPerCommand + testCPUs)
	if commandLimit < 1 {
		return 1
	}
	if commandLimit > workloadCount {
		return workloadCount
	}
	return commandLimit
}

func runConcurrent(ctx context.Context, root string, commands []commandSpec, runner commandRunner) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		index int
		err   error
	}
	results := make(chan result, len(commands))
	var workers sync.WaitGroup
	var cancelOnce sync.Once
	for index, command := range commands {
		workers.Add(1)
		go func(index int, command commandSpec) {
			defer workers.Done()
			err := runner(ctx, root, command)
			if err != nil {
				cancelOnce.Do(cancel)
			}
			results <- result{index: index, err: err}
		}(index, command)
	}
	workers.Wait()
	close(results)
	failures := make([]error, len(commands))
	for result := range results {
		if result.err != nil {
			failures[result.index] = fmt.Errorf("%s: %w", formatCommand(commands[result.index]), result.err)
		}
	}
	var failed []error
	var canceled []error
	for _, err := range failures {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) {
			canceled = append(canceled, err)
			continue
		}
		failed = append(failed, err)
	}
	if len(failed) > 0 {
		return errors.Join(failed...)
	}
	if ctx.Err() != nil {
		return errors.Join(append(canceled, ctx.Err())...)
	}
	return nil
}

func executeCommand(ctx context.Context, root string, spec commandSpec) error {
	command := exec.CommandContext(ctx, spec.Name, spec.Args...)
	command.Dir = filepath.Join(root, filepath.FromSlash(spec.Dir))
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if spec.StdoutFile != "" {
		path := spec.StdoutFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, filepath.FromSlash(path))
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create stdout file directory for %s: %w", formatCommand(spec), err)
		}
		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create stdout file for %s: %w", formatCommand(spec), err)
		}
		defer file.Close()
		command.Stdout = file
	}
	if err := command.Run(); err != nil {
		code := 1
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		}
		if ctx.Err() != nil {
			err = errors.Join(err, ctx.Err())
		}
		return commandFailure{Spec: spec, Code: code, Err: err}
	}
	return nil
}

func formatCommand(spec commandSpec) string {
	return strings.Join(append([]string{spec.Name}, spec.Args...), " ")
}

func ensureRepositoryRoot(root string) error {
	data, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return fmt.Errorf("repository root check failed: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "module "+modulePath {
			return nil
		}
	}
	return fmt.Errorf("repository root check failed: go.mod module is not %s", modulePath)
}

func checkGoFormat(root string) error {
	files := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			name := entry.Name()
			if name == ".git" || name == ".artifacts" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(entry.Name(), ".go") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk Go source for format check: %w", err)
	}
	sort.Strings(files)
	var failures []string
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: read: %v", relativePath(root, path), err))
			continue
		}
		formatted, err := format.Source(data)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: go/format: %v", relativePath(root, path), err))
			continue
		}
		if string(formatted) != strings.ReplaceAll(string(data), "\r\n", "\n") {
			failures = append(failures, relativePath(root, path))
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		return fmt.Errorf("Go source is not gofmt-formatted:\n%s", strings.Join(failures, "\n"))
	}
	return nil
}
