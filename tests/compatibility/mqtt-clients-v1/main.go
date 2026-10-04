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
		_, _ = fmt.Fprintf(os.Stderr, "mqtt-client-compatibility: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("mqtt-client-compatibility", flag.ContinueOnError)
	flags.SetOutput(stderr)
	runID := flags.String("run-id", "", "non-empty path-safe evidence run identifier")
	outputDir := flags.String("output", "", "empty evidence output directory")
	suite := flags.String("suite", "phase1", "compatibility suite: phase1 or drain")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *suite != "phase1" && *suite != "drain" {
		return fmt.Errorf("unsupported suite %q", *suite)
	}
	if *runID == "" || *outputDir == "" {
		return errors.New("--run-id and --output are required")
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}
	profilePath := filepath.Join(workingDirectory, "tests", "compatibility", "mqtt-clients-v1", "profile.json")
	if *suite == "drain" {
		profilePath = filepath.Join(workingDirectory, "tests", "compatibility", "mqtt-clients-v1", "drain-profile.json")
	}
	repoRoot := findRepoRoot(profilePath)
	if repoRoot == "." {
		return errors.New("repository root with go.mod was not found")
	}
	if !filepath.IsAbs(*outputDir) {
		*outputDir = filepath.Join(workingDirectory, *outputDir)
	}
	execute := Run
	if *suite == "drain" {
		execute = RunDrain
	}
	summary, runErr := execute(ctx, runOptions{
		RunID: *runID, OutputDir: *outputDir, ProfilePath: profilePath, RepoRoot: repoRoot,
		Runner: realCommandRunner{},
	})
	if encodeErr := writeSummary(stdout, summary); encodeErr != nil {
		return encodeErr
	}
	return runErr
}

func writeSummary(writer io.Writer, summary Summary) error {
	if summary.RunID == "" {
		return nil
	}
	if _, err := fmt.Fprintf(writer, "%s\n", mustJSON(summary)); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	return nil
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("{\"error\":%q}", err.Error())
	}
	return string(data)
}
