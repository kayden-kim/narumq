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
	if len(commands) != 7 {
		t.Fatalf("command count = %d, want 7", len(commands))
	}
	last := commands[len(commands)-1]
	if !reflect.DeepEqual(last.Args[len(last.Args)-2:], []string{"--base", "HEAD~1"}) {
		t.Fatalf("coverage command args = %v, missing base", last.Args)
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
