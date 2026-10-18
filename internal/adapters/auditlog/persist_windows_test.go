//go:build windows

package auditlog

import (
	"os"
	"path/filepath"
	"testing"
)

// Scenarios: management-api-and-observability.md:MGT-SCN-014, tenancy-and-security.md:SEC-SCN-050
func TestRenameFileDoesNotReplaceExistingTarget(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.jsonl")
	targetPath := filepath.Join(directory, "target.jsonl")
	if err := os.WriteFile(sourcePath, []byte("source"), 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	if err := os.WriteFile(targetPath, []byte("target"), 0o600); err != nil {
		t.Fatalf("write existing target file: %v", err)
	}

	if err := renameFile(sourcePath, targetPath); err == nil {
		t.Fatal("renameFile() succeeded despite an existing target")
	}
	if contents, err := os.ReadFile(targetPath); err != nil || string(contents) != "target" {
		t.Fatalf("existing target contents=%q err=%v, want preserved target", contents, err)
	}
	if contents, err := os.ReadFile(sourcePath); err != nil || string(contents) != "source" {
		t.Fatalf("source contents=%q err=%v, want retained source", contents, err)
	}
}
