package runtimeartifact

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestComputeAggregatesWindowDeltasAndNearestRankPause(t *testing.T) {
	dir := t.TempDir()
	rawPath, phasesPath := writeFixture(t, dir, []map[string]any{
		runtimeRecordMap("2026-09-19T00:00:02Z", 100, 10, 2, 1.0, 0.2, 4.0, 2, []uint64{0, 0, 0}),
		runtimeRecordMap("2026-09-19T00:00:08Z", 700, 16, 5, 1.5, 0.3, 6.0, 4, []uint64{998, 2, 0}),
	}, []map[string]any{
		{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:00Z"},
		{"phase": "measure", "event": "end", "timestamp": "2026-09-19T00:00:10Z"},
	})

	summary, err := Compute(rawPath, phasesPath, "measure", "narumq")
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid || summary.Applicability != "required" || summary.SampleCount != 2 {
		t.Fatalf("Compute() summary = %#v", summary)
	}
	if summary.ElapsedSeconds != 6 || summary.AllocatedBytes != 600 || summary.AllocationCount != 6 || summary.AllocationBytesPerSecond != 100 || summary.AllocationsPerSecond != 1 || summary.GCCycles != 3 || summary.GCCPUSeconds != 0.5 || math.Abs(summary.GCPauseCPUSeconds-0.1) > 1e-9 || summary.RuntimeCPUSeconds != 2 || !summary.CPUMetricsAvailable || summary.GCCPUFraction != 0.25 || summary.GCPauseCount != 1000 || summary.GOMAXPROCSMin != 2 || summary.GOMAXPROCSMax != 4 {
		t.Fatalf("Compute() numeric summary = %#v", summary)
	}
	if summary.GCPauseP50Seconds != 0.001 || summary.GCPauseP95Seconds != 0.001 || summary.GCPauseP99Seconds != 0.001 || summary.GCPauseP999Seconds != 0.01 {
		t.Fatalf("Compute() pause percentiles = %#v", summary)
	}
}
func TestComputeAcceptsBP05IdlePhaseMetadata(t *testing.T) {
	dir := t.TempDir()
	rawPath, phasesPath := writeFixture(t, dir, []map[string]any{
		runtimeRecordMap("2026-09-19T00:00:02Z", 100, 10, 2, 1, 0, 2, 2, []uint64{0, 0, 0}),
		runtimeRecordMap("2026-09-19T00:00:08Z", 200, 12, 2, 1, 0, 4, 2, []uint64{0, 0, 0}),
	}, []map[string]any{
		{"phase": "ramp", "event": "start", "timestamp": "2026-09-19T00:00:00Z", "activeConnections": 0},
		{"phase": "idle", "event": "start", "timestamp": "2026-09-19T00:00:01Z", "activeConnections": 8},
		{"phase": "idle", "event": "end", "timestamp": "2026-09-19T00:00:09Z", "activeConnections": 8},
	})

	summary, err := Compute(rawPath, phasesPath, "idle", "narumq")
	if err != nil || !summary.Valid || summary.Phase != "idle" {
		t.Fatalf("Compute() summary=%#v error=%v, want valid BP-05 idle artifact", summary, err)
	}
}

func TestComputeMarksUnavailableCPUMetricsWithoutInventingValues(t *testing.T) {
	dir := t.TempDir()
	rawPath, phasesPath := writeFixture(t, dir, []map[string]any{
		runtimeRecordMap("2026-09-19T00:00:02Z", 100, 10, 2, 0, 0, 0, 2, []uint64{0, 2, 3}),
		runtimeRecordMap("2026-09-19T00:00:08Z", 700, 16, 5, 0, 0, 0, 2, []uint64{0, 5, 3}),
	}, []map[string]any{
		{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:00Z"},
		{"phase": "measure", "event": "end", "timestamp": "2026-09-19T00:00:10Z"},
	})

	summary, err := Compute(rawPath, phasesPath, "measure", "narumq")
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Valid || summary.CPUMetricsAvailable || summary.CPUAvailabilityReason == "" || summary.RuntimeCPUSeconds != 0 || summary.GCCPUFraction != 0 {
		t.Fatalf("Compute() summary = %#v, want valid unavailable CPU metrics", summary)
	}
}
func TestComputeRejectsGCAdvanceWithoutRuntimeCPU(t *testing.T) {
	dir := t.TempDir()
	rawPath, phasesPath := writeFixture(t, dir, []map[string]any{
		runtimeRecordMap("2026-09-19T00:00:02Z", 100, 10, 2, 0, 0, 0, 2, []uint64{0, 2, 3}),
		runtimeRecordMap("2026-09-19T00:00:08Z", 700, 16, 5, 0.1, 0, 0, 2, []uint64{0, 5, 3}),
	}, []map[string]any{
		{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:00Z"},
		{"phase": "measure", "event": "end", "timestamp": "2026-09-19T00:00:10Z"},
	})

	summary, err := Compute(rawPath, phasesPath, "measure", "narumq")
	if err == nil || summary.Valid || !strings.Contains(summary.FailureReason, "GC CPU counters advanced") {
		t.Fatalf("Compute() summary=%#v error=%v, want inconsistent CPU counters rejected", summary, err)
	}
}

func TestComputeRejectsInvalidRuntimeArtifacts(t *testing.T) {
	baseRecords := []map[string]any{
		runtimeRecordMap("2026-09-19T00:00:02Z", 100, 10, 2, 1.0, 0.2, 4.0, 2, []uint64{0, 2, 3}),
		runtimeRecordMap("2026-09-19T00:00:08Z", 700, 16, 5, 1.5, 0.3, 6.0, 4, []uint64{0, 5, 3}),
	}
	validPhases := []map[string]any{
		{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:00Z"},
		{"phase": "measure", "event": "end", "timestamp": "2026-09-19T00:00:10Z"},
	}
	for _, test := range []struct {
		name       string
		records    []map[string]any
		phaseLines []map[string]any
		trailing   string
	}{
		{
			name:    "one selected sample",
			records: baseRecords[:1],
			phaseLines: []map[string]any{
				{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:00Z"},
				{"phase": "measure", "event": "end", "timestamp": "2026-09-19T00:00:05Z"},
			},
		},
		{
			name: "counter decrease",
			records: []map[string]any{
				runtimeRecordMap("2026-09-19T00:00:02Z", 700, 10, 2, 1.0, 0.2, 4.0, 2, []uint64{0, 2, 3}),
				runtimeRecordMap("2026-09-19T00:00:08Z", 100, 16, 5, 1.5, 0.3, 6.0, 4, []uint64{0, 5, 3}),
			},
			phaseLines: validPhases,
		},
		{
			name: "bucket change",
			records: []map[string]any{
				baseRecords[0],
				func() map[string]any {
					value := runtimeRecordMap("2026-09-19T00:00:08Z", 700, 16, 5, 1.5, 0.3, 6.0, 4, []uint64{0, 5, 3})
					value["gcPauseBucketBoundsSeconds"] = []string{"-Inf", "0.002", "0.01", "+Inf"}
					return value
				}(),
			},
			phaseLines: validPhases,
		},
		{
			name: "infinite pause quantile",
			records: []map[string]any{
				baseRecords[0],
				runtimeRecordMap("2026-09-19T00:00:08Z", 700, 16, 5, 1.5, 0.3, 6.0, 4, []uint64{0, 2, 4}),
			},
			phaseLines: validPhases,
		},
		{
			name:    "missing phase boundary",
			records: baseRecords,
			phaseLines: []map[string]any{
				{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:00Z"},
			},
		},
		{
			name:    "duplicate phase boundary",
			records: baseRecords,
			phaseLines: []map[string]any{
				{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:00Z"},
				{"phase": "measure", "event": "start", "timestamp": "2026-09-19T00:00:01Z"},
				{"phase": "measure", "event": "end", "timestamp": "2026-09-19T00:00:10Z"},
			},
		},
		{
			name:       "trailing JSON",
			records:    baseRecords,
			phaseLines: validPhases,
			trailing:   "not-json\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rawPath, phasesPath := writeFixture(t, t.TempDir(), test.records, test.phaseLines)
			if test.trailing != "" {
				file, err := os.OpenFile(rawPath, os.O_APPEND|os.O_WRONLY, 0o644)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteString(test.trailing); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			summary, err := Compute(rawPath, phasesPath, "measure", "narumq")
			if err == nil || summary.Valid || strings.TrimSpace(summary.FailureReason) == "" {
				t.Fatalf("Compute() summary=%#v err=%v, want invalid artifact", summary, err)
			}
		})
	}
}

func TestNotApplicableRuntimeSummary(t *testing.T) {
	summary := NotApplicable("emqx", "Go runtime/metrics applies only to the NaruMQ Go process")
	if !summary.Valid || summary.Applicability != "not_applicable" || summary.NotApplicableReason == "" || summary.FailureReason != "" || summary.ReleaseGating {
		t.Fatalf("NotApplicable() = %#v", summary)
	}
}

func writeFixture(t *testing.T, dir string, records, phases []map[string]any) (string, string) {
	t.Helper()
	rawPath := filepath.Join(dir, "runtime-metrics.jsonl")
	phasesPath := filepath.Join(dir, "phases.jsonl")
	writeJSONL(t, rawPath, records)
	writeJSONL(t, phasesPath, phases)
	return rawPath, phasesPath
}

func writeJSONL(t *testing.T, path string, values []map[string]any) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func runtimeRecordMap(timestamp string, bytes, objects, cycles uint64, gcCPU, pauseCPU, runtimeCPU float64, gomax uint64, counts []uint64) map[string]any {
	return map[string]any{
		"schemaVersion":              1,
		"collectedAt":                timestamp,
		"heapAllocBytesTotal":        bytes,
		"heapAllocObjectsTotal":      objects,
		"gcCyclesTotal":              cycles,
		"gcCPUSecondsTotal":          gcCPU,
		"gcPauseCPUSecondsTotal":     pauseCPU,
		"runtimeCPUSecondsTotal":     runtimeCPU,
		"gomaxprocsThreads":          gomax,
		"gcPauseBucketBoundsSeconds": []string{"-Inf", "0.001", "0.01", "+Inf"},
		"gcPauseBucketCounts":        counts,
	}
}
