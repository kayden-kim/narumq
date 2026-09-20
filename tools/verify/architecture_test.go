package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateImportGraphFixtures(t *testing.T) {
	valid := loadImportFixture(t, "valid")
	if issues := validateImportGraph(valid); len(issues) != 0 {
		t.Fatalf("valid import graph rejected: %v", issues)
	}

	tests := []struct {
		name string
		want string
	}{
		{name: "domain-to-adapter", want: "narumq/internal/domain/mqtt imports forbidden package"},
		{name: "application-to-adapter", want: "narumq/internal/application/connect imports forbidden package"},
		{name: "adapter-to-adapter", want: "narumq/internal/adapters/tcp imports forbidden package"},
		{name: "non-cmd-to-adapter", want: "narumq/internal/adapters/clock imports forbidden package"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			issues := validateImportGraph(loadImportFixture(t, testCase.name))
			if len(issues) == 0 || !strings.Contains(issues.Error(), testCase.want) {
				t.Fatalf("validateImportGraph() = %v, want %q", issues, testCase.want)
			}
		})
	}
}

func TestValidateImportGraphRejectsGoListPackageError(t *testing.T) {
	issues := validateImportGraph([]importPackage{{ImportPath: "narumq/internal/domain/mqtt", Error: &goListError{Err: "import cycle"}}})
	if len(issues) != 1 || !strings.Contains(issues.Error(), "import cycle") {
		t.Fatalf("issues = %v, want import cycle", issues)
	}
}

func loadImportFixture(t *testing.T, name string) []importPackage {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "architecture", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var packages []importPackage
	if err := json.Unmarshal(data, &packages); err != nil {
		t.Fatal(err)
	}
	return packages
}
