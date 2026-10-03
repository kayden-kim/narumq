package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runCLI(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cluster-storage-chaos: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster-storage-chaos", flag.ContinueOnError)
	flags.SetOutput(stderr)
	runID := flags.String("run-id", "", "path-safe storage-chaos evidence run identifier")
	outputDir := flags.String("output", "", "empty evidence output directory")
	profile := flags.String("profile", localFUSEProfile, "local-fuse-v1 or local-ext4-v1; neither proves production power-loss")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *runID == "" || *outputDir == "" {
		return errors.New("--run-id and --output are required")
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}
	dockerfilePath := filepath.Join(workingDirectory, "tests", "chaos", "cluster-storage-v1", "Dockerfile")
	repoRoot := findRepoRoot(dockerfilePath)
	if repoRoot == "." {
		return errors.New("repository root with go.mod was not found")
	}
	if !filepath.IsAbs(*outputDir) {
		*outputDir = filepath.Join(workingDirectory, *outputDir)
	}
	summary, runErr := Run(ctx, runOptions{
		Profile: *profile,
		RunID:   *runID, OutputDir: *outputDir, RepoRoot: repoRoot, Runner: realCommandRunner{},
	})
	if summary.RunID != "" {
		if err := json.NewEncoder(stdout).Encode(summary); err != nil {
			return fmt.Errorf("write summary to stdout: %w", err)
		}
	}
	return runErr
}

func findRepoRoot(path string) string {
	current, err := filepath.Abs(path)
	if err != nil {
		return "."
	}
	if info, err := os.Stat(current); err == nil && !info.IsDir() {
		current = filepath.Dir(current)
	}
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "."
		}
		current = parent
	}
}
