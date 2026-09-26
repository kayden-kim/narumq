package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

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
	if err := runCI(context.Background(), root, "HEAD~1", func(_ context.Context, _ string, spec commandSpec) error {
		commandsMu.Lock()
		commands = append(commands, spec)
		commandsMu.Unlock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 12 {
		t.Fatalf("command count = %d, want 12", len(commands))
	}
	want := []commandSpec{
		{Name: "go", Args: []string{"vet", "./..."}},
		{Name: "go", Args: []string{"run", "./tools/verify", "traceability"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "architecture"}},
		{Name: "go", Args: []string{"test", "./...", "-parallel=2", "-count=1", "-covermode=atomic", "-coverpkg=./cmd/...,./internal/...", "-coverprofile=.artifacts/verification/coverage.out"}},
		{Name: "go", Args: []string{"test", "-race", "./cmd/...", "./internal/...", "-count=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzDecoder$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/mqtt", "-run=^$", "-fuzz=^FuzzCodecRoundTrip$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/routing", "-run=^$", "-fuzz=^FuzzTopicMatching$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"test", "./internal/domain/session", "-run=^$", "-fuzz=^FuzzLifecycleTransitions$", "-fuzztime=10000x", "-parallel=1"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-delivery.json", "internal/domain/delivery"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-session.json", "internal/domain/session"}},
		{Name: "go", Args: []string{"run", "./tools/verify", "coverage", "--profile", ".artifacts/verification/coverage.out", "--policy", defaultCoveragePolicyPath, "--report", ".artifacts/verification/coverage-summary.json", "--base", "HEAD~1"}},
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
	if len(commands) != 10 {
		t.Fatalf("local command count = %d, want 10", len(commands))
	}
	fuzzCount := 0
	for _, command := range commands {
		if len(command.Args) >= 2 && command.Args[0] == "tool" && command.Args[1] == "gremlins" {
			t.Fatalf("local command included mutation check: %v", command.Args)
		}
		for _, arg := range command.Args {
			if arg == "--base" {
				t.Fatalf("local command received CI coverage base: %v", command)
			}
			if strings.HasPrefix(arg, "-fuzz=") {
				fuzzCount++
			}
		}
	}
	if fuzzCount != 4 {
		t.Fatalf("local fuzz command count = %d, want 4", fuzzCount)
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
