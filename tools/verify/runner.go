package main

import (
	"context"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type commandSpec struct {
	Name string
	Args []string
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
}

func runLocal(ctx context.Context, root string, runner commandRunner) error {
	return runVerification(ctx, root, verificationOptions{}, runner)
}

func runCI(ctx context.Context, root, base string, runner commandRunner) error {
	if strings.TrimSpace(base) == "" {
		return fmt.Errorf("ci requires --base <git-ref>")
	}
	return runVerification(ctx, root, verificationOptions{CI: true, Base: base}, runner)
}

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
	commands := []commandSpec{
		{Name: "go", Args: []string{"vet", "./..."}},
		{Name: "go", Args: []string{"test", "./...", "-count=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzDecoder$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzCodecRoundTrip$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/routing", "-run=^$", "-fuzz=^FuzzTopicMatching$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/session", "-run=^$", "-fuzz=^FuzzLifecycleTransitions$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "-race", "./...", "-count=1"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "traceability"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "architecture"}},
		{Name: "go", Args: []string{"test", "./cmd/...", "./internal/...", "./tests/integration/...", "-count=1", "-covermode=atomic", "-coverpkg=./cmd/...,./internal/...", "-coverprofile=.artifacts/verification/coverage.out"}},
	}
	coverageArgs := []string{"run", "./tools/verify", "coverage", "--profile", ".artifacts/verification/coverage.out", "--policy", defaultCoveragePolicyPath, "--report", ".artifacts/verification/coverage-summary.json"}
	if options.CI {
		coverageArgs = append(coverageArgs, "--base", options.Base)
	}
	commands = append(commands,
		commandSpec{Name: "go", Args: coverageArgs},
		commandSpec{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-delivery.json", "internal/domain/delivery"}},
		commandSpec{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-session.json", "internal/domain/session"}},
	)
	for _, command := range commands {
		if err := runner(ctx, root, command); err != nil {
			return fmt.Errorf("verification stopped after %s: %w", formatCommand(command), err)
		}
	}
	return nil
}

func executeCommand(ctx context.Context, root string, spec commandSpec) error {
	command := exec.CommandContext(ctx, spec.Name, spec.Args...)
	command.Dir = root
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		code := 1
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
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
		if string(formatted) != string(data) {
			failures = append(failures, relativePath(root, path))
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		return fmt.Errorf("Go source is not gofmt-formatted:\n%s", strings.Join(failures, "\n"))
	}
	return nil
}
