package main

import (
	"fmt"
	"testing"
	"time"
)

// Scenarios: performance-and-verification.md:PRF-SCN-121
func TestScenarioExecutionScopeUsesGoBuildConstraints(t *testing.T) {
	for _, target := range []struct {
		os, arch, flags string
	}{
		{"linux", "amd64", ""},
		{"windows", "amd64", ""},
		{"linux", "arm64", ""},
		{"linux", "amd64", "-tags=scenario_feature"},
	} {
		t.Run(target.os+"/"+target.arch+"/"+target.flags, func(t *testing.T) {
			t.Setenv("GOOS", target.os)
			t.Setenv("GOARCH", target.arch)
			t.Setenv("GOFLAGS", target.flags)
			t.Setenv("CGO_ENABLED", "0")
			files := map[string]string{
				"go.mod":          "module " + scenarioFixtureModule + "\n\ngo 1.26\n",
				"results/go.json": `{"Action":"pass","Package":"` + scenarioFixtureModule + `/pkg"}` + "\n",
			}
			cases := []struct {
				file, constraint, name string
				active                 bool
			}{
				{"platform_windows_test.go", "", "Windows", target.os == "windows"},
				{"platform_linux_test.go", "", "Linux", target.os == "linux"},
				{"machine_arm64_test.go", "", "ARM", target.arch == "arm64"},
				{"tagged_test.go", "//go:build scenario_feature && linux\n\n", "Tagged", target.os == "linux" && target.flags != ""},
				{"ordinary_test.go", "", "Missing", true},
				{"skip_test.go", "", "Skipped", true},
			}
			ids := make([]string, 0, len(cases))
			for index, test := range cases {
				id := fmt.Sprintf("AAA-SCN-%03d", index+1)
				ids = append(ids, id)
				files["pkg/"+test.file] = test.constraint + "package pkg\n\nimport \"testing\"\n\n// Scenarios: a.md:" + id + "\nfunc Test" + test.name + "(t *testing.T) {}\n"
			}
			files["docs/specs/a.md"] = scenarioTable(ids...)
			files["results/go.json"] += `{"Action":"skip","Package":"` + scenarioFixtureModule + `/pkg","Test":"TestSkipped"}` + "\n"
			root := writeScenarioFixture(t, files)
			report, err := runScenarios(root, scenarioOptions{GoTestJSON: []string{"results/go.json"}, RequireExecuted: true}, time.Unix(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			if report.Summary.Errors != 0 || report.Summary.Unlinked != 0 {
				t.Fatalf("excluded source linkage lost: %+v problems=%v", report.Summary, report.Problems)
			}
			for index, entry := range report.Specs[0].Scenarios {
				if len(entry.Tests) != 1 || entry.Tests[0].InScope != cases[index].active {
					t.Fatalf("%s: tests=%+v, want inScope=%v", entry.ID, entry.Tests, cases[index].active)
				}
				wantReason := scenarioReasonOutsideGate
				if cases[index].active {
					wantReason = scenarioReasonNoResult
				}
				if cases[index].name == "Skipped" {
					wantReason = scenarioReasonSkipped
				}
				if entry.Status != scenarioStatusNotRun || entry.Reason != wantReason {
					t.Fatalf("%s: status=%s reason=%s, want not run/%s", entry.ID, entry.Status, entry.Reason, wantReason)
				}
			}
			if !report.Summary.hasProblems() || report.Summary.NotExecuted < 2 {
				t.Fatalf("ordinary missing/skipped tests did not fail execution check: %+v", report.Summary)
			}
		})
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-121
func TestScenarioExecutionScopePreservesExplicitForeignPlatformResults(t *testing.T) {
	t.Setenv("GOOS", "linux")
	t.Setenv("GOARCH", "amd64")
	t.Setenv("GOFLAGS", "")
	for _, input := range []string{"go", "manual"} {
		for _, status := range []string{testResultPass, testResultFail} {
			t.Run(input+"/"+status, func(t *testing.T) {
				files := map[string]string{
					"go.mod":                       "module " + scenarioFixtureModule + "\n\ngo 1.26\n",
					"docs/specs/a.md":              scenarioTable("AAA-SCN-001"),
					"pkg/platform_windows_test.go": "package pkg\n\nimport \"testing\"\n\n// Scenarios: a.md:AAA-SCN-001\nfunc TestWindows(t *testing.T) {}\n",
					"results/go.json":              `{"Action":"pass","Package":"` + scenarioFixtureModule + `/pkg"}` + "\n",
				}
				options := scenarioOptions{GoTestJSON: []string{"results/go.json"}, RequireExecuted: true}
				if input == "go" {
					files["results/go.json"] += `{"Action":"` + status + `","Package":"` + scenarioFixtureModule + `/pkg","Test":"TestWindows"}` + "\n"
				} else {
					files["results/manual.jsonl"] = `{"test":"` + scenarioFixtureModule + `/pkg.TestWindows","status":"` + status + `"}` + "\n"
					options.Results = []string{"results/manual.jsonl"}
				}
				report, err := runScenarios(writeScenarioFixture(t, files), options, time.Unix(0, 0))
				if err != nil {
					t.Fatal(err)
				}
				entry := report.Specs[0].Scenarios[0]
				want := scenarioStatusPassed
				if status == testResultFail {
					want = scenarioStatusFailed
				}
				if entry.Status != want || !entry.Tests[0].InScope || report.Summary.hasProblems() != (status == testResultFail) {
					t.Fatalf("foreign result hidden: entry=%+v summary=%+v", entry, report.Summary)
				}
			})
		}
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-121
func TestScenarioLinkScanRetainsExcludedMetadataWithoutLoadingNestedDependencies(t *testing.T) {
	t.Setenv("GOOS", "linux")
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOPROXY", "off")
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n\ngo 1.26\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001"),
		"nested/go.mod":   "module example.test/nested\n\ngo 1.26\n\nrequire example.invalid/unavailable v0.0.1\n",
		"nested/platform_windows_test.go": "package nested\n\nimport _ \"example.invalid/unavailable\"\n\n" +
			"// Scenarios: a.md:AAA-SCN-001\nfunc TestLinked(t *testing.T) {}\n\n" +
			"// Scenarios: invalid reference\nfunc TestMalformed(t *testing.T) {}\n\n" +
			"// Scenarios: a.md:AAA-SCN-999\nfunc TestUnknown(t *testing.T) {}\n",
	})
	report, err := runScenarios(root, scenarioOptions{}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Errors != 2 || report.Summary.Unlinked != 0 || len(report.Specs[0].Scenarios[0].Tests) != 1 {
		t.Fatalf("excluded metadata was lost or dependencies were required: summary=%+v problems=%v", report.Summary, report.Problems)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-121
func TestScenarioPackageErrorsDoNotExemptOrdinaryMissingResults(t *testing.T) {
	t.Setenv("GOOS", "linux")
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOPROXY", "off")
	root := writeScenarioFixture(t, map[string]string{
		"go.mod":          "module " + scenarioFixtureModule + "\n\ngo 1.26\n",
		"docs/specs/a.md": scenarioTable("AAA-SCN-001"),
		"pkg/ordinary_test.go": "package pkg\n\nimport _ \"example.invalid/unavailable\"\n\n" +
			"// Scenarios: a.md:AAA-SCN-001\nfunc TestMissing(t *testing.T) {}\n",
		"pkg/conflict.go": "package different\n",
		"results/go.json": `{"Action":"fail","Package":"` + scenarioFixtureModule + `/pkg"}` + "\n",
	})
	report, err := runScenarios(root, scenarioOptions{GoTestJSON: []string{"results/go.json"}, RequireExecuted: true}, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !report.Summary.hasProblems() || report.Summary.NotExecuted != 1 || !report.Specs[0].Scenarios[0].Tests[0].InScope {
		t.Fatalf("package error exempted an ordinary missing result: %+v", report.Summary)
	}
}
