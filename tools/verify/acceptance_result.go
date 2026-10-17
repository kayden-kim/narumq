package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const defaultAcceptanceResultsPath = ".artifacts/verification/scenarios/go-acceptance-results.jsonl"

// acceptanceResultRecord is one line of the --results file read by `verify scenarios`.
type acceptanceResultRecord struct {
	Test        string `json:"test"`
	Status      string `json:"status"`
	Commit      string `json:"commit"`
	Environment string `json:"environment"`
	Command     string `json:"command"`
	FinishedAt  string `json:"finishedAt"`
}

// acceptanceKey names a non-Test function of this package the way `verify scenarios` keys it.
func acceptanceKey(function string) string {
	return modulePath + "/tools/verify." + function
}

// appendAcceptanceResult appends one result record. Only the real outcome of an executed acceptance
// function is recorded; callers never write a pass for a run that did not finish.
func appendAcceptanceResult(root, path, function, status, command string, finishedAt time.Time) error {
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, filepath.FromSlash(path))
	}
	record := acceptanceResultRecord{
		Test:        acceptanceKey(function),
		Status:      status,
		Commit:      gitCommitDescription(root),
		Environment: fmt.Sprintf("%s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH),
		Command:     command,
		FinishedAt:  finishedAt.UTC().Format(time.RFC3339),
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode acceptance result: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create acceptance result directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open acceptance results %s: %w", path, err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("write acceptance results %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close acceptance results %s: %w", path, err)
	}
	return nil
}

// gitCommitDescription returns the HEAD commit, with +dirty for uncommitted changes, or unknown.
func gitCommitDescription(root string) string {
	head, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	commit := strings.TrimSpace(string(head))
	status, err := exec.Command("git", "-C", root, "status", "--porcelain").Output()
	if err == nil && strings.TrimSpace(string(status)) != "" {
		commit += "+dirty"
	}
	return commit
}

// finishAcceptance records the real outcome of an acceptance subcommand in the results file and
// returns the process exit code. A run that could not be recorded fails.
func finishAcceptance(root, resultsPath, function, command, name string, runErr error) int {
	status := "pass"
	if runErr != nil {
		status = "fail"
	}
	recordErr := appendAcceptanceResult(root, resultsPath, function, status, command, time.Now())
	if runErr != nil {
		if recordErr != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, recordErr)
		}
		return printCommandError(name, runErr)
	}
	if recordErr != nil {
		return printCommandError(name, recordErr)
	}
	fmt.Printf("%s: PASS\n", name)
	return 0
}
