package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRunLocalExecutesOrderedCommandsAndStopsAtFirstFailure(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n\n go 1.26.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	commands := make([]string, 0)
	failure := errors.New("test command failed")
	runner := func(_ context.Context, _ string, spec commandSpec) error {
		commands = append(commands, formatCommand(spec))
		if len(commands) == 2 {
			return failure
		}
		return nil
	}
	if err := runLocal(context.Background(), root, runner); err == nil || !strings.Contains(err.Error(), "stopped after") {
		t.Fatalf("runLocal() error = %v, want first failure", err)
	}
	if len(commands) != 2 {
		t.Fatalf("commands = %v, want two commands before stop", commands)
	}
	if !strings.HasPrefix(commands[0], "go vet") || !strings.HasPrefix(commands[1], "go test ./...") {
		t.Fatalf("commands = %v, unexpected order", commands)
	}
}

func TestRunCIRequiresBaseAndPropagatesBaseToCoverage(t *testing.T) {
	if err := runCI(context.Background(), t.TempDir(), "", func(context.Context, string, commandSpec) error { return nil }); err == nil || !strings.Contains(err.Error(), "--base") {
		t.Fatalf("runCI() without base error = %v, want required base", err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module narumq\n\ngo 1.26.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var commands []commandSpec
	if err := runCI(context.Background(), root, "HEAD~1", func(_ context.Context, _ string, spec commandSpec) error {
		commands = append(commands, spec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 13 {
		t.Fatalf("command count = %d, want 13", len(commands))
	}
	want := []commandSpec{
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
		{Name: "go", Args: []string{"run", "./tools/verify", "coverage", "--profile", ".artifacts/verification/coverage.out", "--policy", defaultCoveragePolicyPath, "--report", ".artifacts/verification/coverage-summary.json", "--base", "HEAD~1"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-delivery.json", "internal/domain/delivery"}},
		{Name: "go", Args: []string{"tool", "gremlins", "unleash", "--config=tests/verification/gremlins.yaml", "--output=.artifacts/verification/mutation-session.json", "internal/domain/session"}},
	}
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
			if index+1 >= len(command.Args) || command.Args[index+1] != "HEAD~1" {
				t.Fatalf("base argument = %v, want HEAD~1", command.Args)
			}
			if command.Args[0] == "tool" {
				t.Fatalf("mutation command received coverage base: %v", command.Args)
			}
		}
	}
	if coverageBaseCount != 1 {
		t.Fatalf("coverage base count = %d, want 1", coverageBaseCount)
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
