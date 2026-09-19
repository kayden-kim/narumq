package runtimemetrics

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSamplerWritesImmediateAndFinalSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	sampler, err := New(path, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sampler.Run(ctx) }()

	var beforeCancel []runtimeMetricsRecord
	deadline := time.Now().Add(time.Second)
	for {
		beforeCancel, err = readRuntimeRecords(path)
		if err == nil && len(beforeCancel) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("immediate sample = %d records, error=%v; want exactly one before cancellation", len(beforeCancel), err)
		}
		time.Sleep(time.Millisecond)
	}
	firstAt, err := time.Parse(time.RFC3339Nano, beforeCancel[0].CollectedAt)
	if err != nil {
		t.Fatal(err)
	}
	for !time.Now().After(firstAt) {
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Sampler.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Sampler.Run() did not stop")
	}

	records, err := readRuntimeRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("sample count after cancellation = %d, want immediate plus exactly one final row", len(records))
	}
	finalAt, err := time.Parse(time.RFC3339Nano, records[1].CollectedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !finalAt.After(firstAt) {
		t.Fatalf("final sample timestamp = %s, want newer than immediate sample %s", finalAt, firstAt)
	}
	assertRuntimeRecords(t, records)
}

func TestSamplerWritesPeriodicSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	sampler, err := New(path, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sampler.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for {
		records, readErr := readRuntimeRecords(path)
		if readErr == nil && len(records) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("periodic sample count = %d, error=%v; want at least 2", len(records), readErr)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Sampler.Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Sampler.Run() did not stop")
	}
	records, err := readRuntimeRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) < 2 {
		t.Fatalf("sample count = %d, want at least 2 periodic rows", len(records))
	}
	assertRuntimeRecords(t, records)
}

func TestSamplerSkipsDuplicateTimestamp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	sampler, err := New(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now()
	if err := sampler.writeSample(timestamp); err != nil {
		t.Fatal(err)
	}
	before, err := readRuntimeRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := sampler.writeSample(timestamp); err != nil {
		t.Fatal(err)
	}
	after, err := readRuntimeRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("duplicate timestamp rows before=%d after=%d, want one row", len(before), len(after))
	}
	if err := sampler.close(); err != nil {
		t.Fatal(err)
	}
}

func assertRuntimeRecords(t *testing.T, records []runtimeMetricsRecord) {
	t.Helper()
	if len(records) == 0 {
		t.Fatal("runtime metrics records are empty")
	}
	for index, record := range records {
		if record.SchemaVersion != schemaVersion {
			t.Fatalf("record %d schemaVersion = %d", index, record.SchemaVersion)
		}
		if _, err := time.Parse(time.RFC3339Nano, record.CollectedAt); err != nil {
			t.Fatalf("record %d collectedAt = %q: %v", index, record.CollectedAt, err)
		}
		if len(record.GCPauseBucketBoundsSeconds) != len(record.GCPauseBucketCounts)+1 {
			t.Fatalf("record %d histogram lengths = %d/%d", index, len(record.GCPauseBucketBoundsSeconds), len(record.GCPauseBucketCounts))
		}
		if index > 0 {
			previous := records[index-1]
			if record.HeapAllocBytesTotal < previous.HeapAllocBytesTotal || record.HeapAllocObjectsTotal < previous.HeapAllocObjectsTotal || record.GCCyclesTotal < previous.GCCyclesTotal || record.GCCPUSecondsTotal < previous.GCCPUSecondsTotal || record.GCPauseCPUSecondsTotal < previous.GCPauseCPUSecondsTotal || record.RuntimeCPUSecondsTotal < previous.RuntimeCPUSecondsTotal {
				t.Fatalf("record %d cumulative metrics decreased: previous=%#v current=%#v", index, previous, record)
			}
			if !mustParseTimestamp(t, record.CollectedAt).After(mustParseTimestamp(t, previous.CollectedAt)) {
				t.Fatalf("record %d timestamp did not increase", index)
			}
			for bucket, count := range record.GCPauseBucketCounts {
				if count < previous.GCPauseBucketCounts[bucket] {
					t.Fatalf("record %d histogram bucket %d decreased", index, bucket)
				}
			}
		}
	}
}

func mustParseTimestamp(t *testing.T, value string) time.Time {
	t.Helper()
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return timestamp
}

func readRuntimeRecords(path string) ([]runtimeMetricsRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	var records []runtimeMetricsRecord
	for {
		var record runtimeMetricsRecord
		if err := decoder.Decode(&record); err == io.EOF {
			return records, nil
		} else if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	for _, test := range []struct {
		name     string
		path     string
		interval time.Duration
	}{
		{name: "empty path", path: "", interval: time.Second},
		{name: "zero interval", path: path, interval: 0},
		{name: "negative interval", path: path, interval: -time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.path, test.interval); err == nil {
				t.Fatal("New() accepted invalid configuration")
			}
		})
	}
}

func TestNewCreatesTruncatedOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-metrics.jsonl")
	if err := os.WriteFile(path, []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sampler, err := New(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := sampler.close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("truncated output has %d bytes", len(data))
	}
}
