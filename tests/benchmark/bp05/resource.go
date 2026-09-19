package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

// ResourceSummary records signed memory change across the BP-05 idle window.
type ResourceSummary struct {
	SchemaVersion                int     `json:"schemaVersion"`
	BaselineSampleCount          int     `json:"baselineSampleCount"`
	BaselineMemoryBytes          int64   `json:"baselineMemoryBytes"`
	PlateauSampleCount           int     `json:"plateauSampleCount"`
	PlateauMemoryBytes           int64   `json:"plateauMemoryBytes"`
	IncrementalMemoryBytes       int64   `json:"incrementalMemoryBytes"`
	MemoryBytesPerIdleConnection float64 `json:"memoryBytesPerIdleConnection"`
	IdleSurvivors                int     `json:"idleSurvivors"`
	WorkloadValid                bool    `json:"workloadValid"`
	Valid                        bool    `json:"valid"`
	ReleaseGating                bool    `json:"releaseGating"`
	FailureReason                string  `json:"failureReason,omitempty"`
}

type phaseWindowEvent struct {
	Phase             string `json:"phase"`
	Event             string `json:"event"`
	Timestamp         string `json:"timestamp"`
	ActiveConnections int    `json:"activeConnections"`
}

func ComputeResourceSummary(statsPath, phasesPath, workloadSummaryPath string) (ResourceSummary, error) {
	var workload WorkloadSummary
	workloadData, err := os.ReadFile(workloadSummaryPath)
	if err != nil {
		return ResourceSummary{}, fmt.Errorf("read workload summary: %w", err)
	}
	if err := json.Unmarshal(workloadData, &workload); err != nil {
		return ResourceSummary{}, fmt.Errorf("decode workload summary: %w", err)
	}
	stats, err := environment.ReadStats(statsPath)
	if err != nil {
		return ResourceSummary{}, err
	}
	idleStart, idleEnd, rampStart, err := readPhaseWindow(phasesPath)
	if err != nil {
		return ResourceSummary{}, err
	}
	baseline := make([]int64, 0, len(stats))
	plateau := make([]int64, 0, len(stats))
	for _, sample := range stats {
		collectedAt, err := time.Parse(time.RFC3339Nano, sample.CollectedAt)
		if err != nil {
			return ResourceSummary{}, fmt.Errorf("parse stats timestamp %q: %w", sample.CollectedAt, err)
		}
		if !collectedAt.After(rampStart) {
			baseline = append(baseline, sample.MemoryUsageBytes)
		}
		if !collectedAt.Before(idleStart) && !collectedAt.After(idleEnd) {
			plateau = append(plateau, sample.MemoryUsageBytes)
		}
	}
	summary := ResourceSummary{
		SchemaVersion:       1,
		BaselineSampleCount: len(baseline),
		PlateauSampleCount:  len(plateau),
		IdleSurvivors:       workload.IdleSurvivors,
		WorkloadValid:       workload.Valid,
		ReleaseGating:       false,
	}
	if len(baseline) < 2 {
		summary.FailureReason = "at least two baseline memory samples are required"
		return summary, errors.New(summary.FailureReason)
	}
	if len(plateau) < 2 {
		summary.FailureReason = "at least two plateau memory samples are required"
		return summary, errors.New(summary.FailureReason)
	}
	if workload.IdleSurvivors <= 0 {
		summary.FailureReason = "idleSurvivors must be greater than zero"
		return summary, errors.New(summary.FailureReason)
	}
	summary.BaselineMemoryBytes = medianMemory(baseline)
	summary.PlateauMemoryBytes = medianMemory(plateau)
	summary.IncrementalMemoryBytes = summary.PlateauMemoryBytes - summary.BaselineMemoryBytes
	summary.MemoryBytesPerIdleConnection = float64(summary.IncrementalMemoryBytes) / float64(summary.IdleSurvivors)
	summary.Valid = true
	return summary, nil
}

func readPhaseWindow(path string) (time.Time, time.Time, time.Time, error) {
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, fmt.Errorf("open phases %q: %w", path, err)
	}
	defer file.Close()
	var rampStart, idleStart, idleEnd time.Time
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1<<20)
	line := 0
	for scanner.Scan() {
		line++
		var event phaseWindowEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return time.Time{}, time.Time{}, time.Time{}, fmt.Errorf("decode phases line %d: %w", line, err)
		}
		timestamp, err := time.Parse(time.RFC3339Nano, event.Timestamp)
		if err != nil {
			return time.Time{}, time.Time{}, time.Time{}, fmt.Errorf("parse phases line %d timestamp: %w", line, err)
		}
		switch {
		case event.Phase == "ramp" && event.Event == "start":
			rampStart = timestamp
		case event.Phase == "idle" && event.Event == "start":
			idleStart = timestamp
		case event.Phase == "idle" && event.Event == "end":
			idleEnd = timestamp
		}
	}
	if err := scanner.Err(); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, fmt.Errorf("read phases: %w", err)
	}
	if rampStart.IsZero() || idleStart.IsZero() || idleEnd.IsZero() || idleEnd.Before(idleStart) {
		return time.Time{}, time.Time{}, time.Time{}, errors.New("ramp start and idle start/end phase events are required")
	}
	return idleStart, idleEnd, rampStart, nil
}

func medianMemory(values []int64) int64 {
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return sorted[middle-1] + (sorted[middle]-sorted[middle-1])/2
}
