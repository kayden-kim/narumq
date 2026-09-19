package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNearestRankPercentiles(t *testing.T) {
	values := make([]int64, 100)
	for index := range values {
		values[index] = int64(index + 1)
	}
	percentiles := nearestRankPercentiles(values)
	if percentiles.P50Nanoseconds != 50 || percentiles.P95Nanoseconds != 95 || percentiles.P99Nanoseconds != 99 || percentiles.P999Nanoseconds != 100 {
		t.Fatalf("nearestRankPercentiles() = %#v, want p50=50 p95=95 p99=99 p999=100", percentiles)
	}
}

func TestCollectorExcludesWarmupAndCountsDuplicateMissing(t *testing.T) {
	profile := validProfile()
	outputDir := t.TempDir()
	collector, err := NewCollector(profile, "narumq", outputDir)
	if err != nil {
		t.Fatal(err)
	}
	collector.SetOffered(3)
	base := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	collector.RecordSent(1, phaseWarmup, base, base.Add(time.Millisecond), time.Millisecond, false)
	collector.RecordSent(2, phaseMeasurement, base, base.Add(2*time.Millisecond), 2*time.Millisecond, false)
	collector.RecordSent(3, phaseMeasurement, base, base.Add(3*time.Millisecond), 3*time.Millisecond, false)
	collector.RecordSent(4, phaseMeasurement, base, base.Add(4*time.Millisecond), 4*time.Millisecond, false)
	if err := collector.RecordDelivery(1, base.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := collector.RecordDelivery(2, base.Add(4*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := collector.RecordDelivery(2, base.Add(5*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	summary, err := collector.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if summary.Sent != 3 || summary.Delivered != 1 || summary.Duplicate != 1 || summary.Missing != 2 {
		t.Fatalf("summary counts = %#v", summary)
	}
	if summary.LatencyNanoseconds.P50Nanoseconds != int64(2*time.Millisecond) {
		t.Fatalf("measurement percentile = %#v, warmup was not excluded", summary.LatencyNanoseconds)
	}
	if summary.Valid {
		t.Fatal("invalid duplicate/missing run marked valid")
	}
	data, err := os.ReadFile(filepath.Join(outputDir, "samples.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, byteValue := range data {
		if byteValue == '\n' {
			lines++
		}
	}
	if lines != 3 {
		t.Fatalf("samples.jsonl lines = %d, want 3", lines)
	}
}

func TestCollectorCountsOutOfRangeAndGeneratorOverload(t *testing.T) {
	profile := validProfile()
	collector, err := NewCollector(profile, "emqx", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	collector.SetOffered(1)
	base := time.Now()
	collector.RecordSent(1, phaseMeasurement, base, base, time.Second, true)
	if err := collector.RecordDelivery(999, base.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	summary, err := collector.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if summary.OutOfRange != 1 || summary.GeneratorOverload != 1 || summary.Missing != 1 || summary.ErrorCount != 3 {
		t.Fatalf("summary error counts = %#v", summary)
	}
}

func TestWriteSummaryUsesStableContract(t *testing.T) {
	outputDir := t.TempDir()
	want := Summary{SchemaVersion: 1, Target: "narumq", Purpose: purposeSmoke, ReleaseGating: false}
	if err := WriteSummary(outputDir, want); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(outputDir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got Summary
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Target != "narumq" || got.ReleaseGating {
		t.Fatalf("summary.json = %#v", got)
	}
}
