package main

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Traceability: PRD TEST-10; traceability CLI rejects unmapped and stale P0 requirement exceptions.
// Scenarios: performance-and-verification.md:PRF-SCN-068
func TestRunTraceabilityRejectsUnmappedAndStaleP0Exceptions(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "docs", "requirements", "PRD.md"), []byte(
		"| ID | 우선순위 | 요구사항 |\n"+
			"| --- | --- | --- |\n"+
			"| API-01 | P0 | Metrics endpoint |\n"+
			"| MQTT-11 | P0 | MQTT behavior |\n"+
			"| TEST-04 | P1 | Test behavior |\n"+
			"| UI-01 | P0 | Dashboard behavior |\n"))
	writeFixtureFile(t, filepath.Join(root, "docs", "TODO.md"), []byte("# TODO\n\n## fixture\n"))
	writeFixtureFile(t, filepath.Join(root, "internal", "domain", "mqtt", "example_test.go"),
		[]byte("package mqtt\n\n// Traceability: PRD MQTT-11; MQTT-1.2.7-1\nfunc TestExample() {}\n"))
	writeFixtureFile(t, filepath.Join(root, "internal", "api", "metrics_test.go"),
		[]byte("package api\n\n// Traceability: API-01; x\nfunc TestMetrics() {}\n"))
	matrix, err := os.ReadFile(filepath.Join("testdata", "traceability", "valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, filepath.Join(root, "matrix.json"), matrix)

	tests := []struct {
		name       string
		exceptions string
		want       string
		notWant    string
	}{
		{
			name:       "UI-01 registration removed",
			exceptions: `{"schemaVersion":1,"exceptions":[]}`,
			want:       "P0 requirement UI-01 has no Traceability test and no exception",
		},
		{
			name: "API-01 stale exception",
			exceptions: `{"schemaVersion":1,"exceptions":[` +
				`{"requirementId":"API-01","reason":"fixture","tracking":"docs/TODO.md#fixture"},` +
				`{"requirementId":"UI-01","reason":"fixture","tracking":"docs/TODO.md#fixture"}]}`,
			want:    "P0 requirement API-01 is mapped by 1 test(s) but still listed",
			notWant: "UI-01",
		},
	}
	t.Chdir(root)
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			writeFixtureFile(t, filepath.Join(root, "exceptions.json"), []byte(testCase.exceptions))
			code, stdout, stderr := runCapturingOutput(t, "traceability",
				"--matrix", "matrix.json", "--prd", filepath.Join("docs", "requirements", "PRD.md"), "--p0-exceptions", "exceptions.json")
			if code != 1 {
				t.Fatalf("exit code = %d, want 1; stdout=%q stderr=%q", code, stdout, stderr)
			}
			if !strings.Contains(stderr, testCase.want) {
				t.Fatalf("stderr = %q, want substring %q", stderr, testCase.want)
			}
			if testCase.notWant != "" && strings.Contains(stderr, testCase.notWant) {
				t.Fatalf("stderr = %q, must not mention %q", stderr, testCase.notWant)
			}
			if strings.Contains(stdout, "traceability: PASS") {
				t.Fatalf("stdout = %q, must not report PASS", stdout)
			}
		})
	}
}

var traceabilitySummaryPattern = regexp.MustCompile(`^traceability: PASS \(P0 mapped (\d+)/(\d+), exceptions (\d+)\)$`)

// Traceability: PRD TEST-10; repository traceability maps each P0 requirement or keeps one valid exception.
// Scenarios: performance-and-verification.md:PRF-SCN-069, performance-and-verification.md:PRF-SCN-096
func TestRunTraceabilityPassesAtRepositoryRoot(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	code, stdout, stderr := runCapturingOutput(t, "traceability")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stdout=%q stderr=%q", code, stdout, stderr)
	}
	match := traceabilitySummaryPattern.FindStringSubmatch(strings.TrimSpace(stdout))
	if match == nil {
		t.Fatalf("stdout = %q, want traceability PASS summary", stdout)
	}
	mapped, _ := strconv.Atoi(match[1])
	total, _ := strconv.Atoi(match[2])
	exceptions, _ := strconv.Atoi(match[3])
	if total == 0 || mapped+exceptions != total {
		t.Fatalf("P0 mapped %d + exceptions %d != total %d: every P0 requirement must belong to exactly one of the two", mapped, exceptions, total)
	}
}

// runCapturingOutput runs the verify CLI in the current directory and returns its exit code, stdout and stderr.
func runCapturingOutput(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdoutReader.Close()
	defer stdoutWriter.Close()
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stderrReader.Close()
	defer stderrWriter.Close()
	originalStdout, originalStderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = stdoutWriter, stderrWriter
	stdoutDone, stderrDone := make(chan string, 1), make(chan string, 1)
	collect := func(reader *os.File, done chan<- string) {
		data, _ := io.ReadAll(reader)
		done <- string(data)
	}
	go collect(stdoutReader, stdoutDone)
	go collect(stderrReader, stderrDone)
	code := func() int {
		defer func() {
			os.Stdout, os.Stderr = originalStdout, originalStderr
			_ = stdoutWriter.Close()
			_ = stderrWriter.Close()
		}()
		return run(args)
	}()
	return code, <-stdoutDone, <-stderrDone
}
