// Package runtimeartifact validates and aggregates NaruMQ Go runtime metric artifacts.
package runtimeartifact

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"time"
)

const schemaVersion = 1

// Summary is the stable runtime measurement artifact contract for one phase.
type Summary struct {
	SchemaVersion            int     `json:"schemaVersion"`
	Target                   string  `json:"target"`
	Applicability            string  `json:"applicability"`
	Phase                    string  `json:"phase"`
	SampleCount              int     `json:"sampleCount"`
	StartedAt                string  `json:"startedAt"`
	FinishedAt               string  `json:"finishedAt"`
	ElapsedSeconds           float64 `json:"elapsedSeconds"`
	AllocatedBytes           uint64  `json:"allocatedBytes"`
	AllocationCount          uint64  `json:"allocationCount"`
	AllocationBytesPerSecond float64 `json:"allocationBytesPerSecond"`
	AllocationsPerSecond     float64 `json:"allocationsPerSecond"`
	GCCycles                 uint64  `json:"gcCycles"`
	GCCPUSeconds             float64 `json:"gcCPUSeconds"`
	GCPauseCPUSeconds        float64 `json:"gcPauseCPUSeconds"`
	RuntimeCPUSeconds        float64 `json:"runtimeCPUSeconds"`
	CPUMetricsAvailable      bool    `json:"cpuMetricsAvailable"`
	CPUAvailabilityReason    string  `json:"cpuMetricsReason,omitempty"`
	GCCPUFraction            float64 `json:"gcCPUFraction"`
	GCPauseCount             uint64  `json:"gcPauseCount"`
	GCPauseP50Seconds        float64 `json:"gcPauseP50Seconds"`
	GCPauseP95Seconds        float64 `json:"gcPauseP95Seconds"`
	GCPauseP99Seconds        float64 `json:"gcPauseP99Seconds"`
	GCPauseP999Seconds       float64 `json:"gcPauseP999Seconds"`
	GOMAXPROCSMin            uint64  `json:"gomaxprocsMin"`
	GOMAXPROCSMax            uint64  `json:"gomaxprocsMax"`
	Valid                    bool    `json:"valid"`
	ReleaseGating            bool    `json:"releaseGating"`
	FailureReason            string  `json:"failureReason,omitempty"`
	NotApplicableReason      string  `json:"notApplicableReason,omitempty"`
}

type runtimeRecord struct {
	SchemaVersion              *int      `json:"schemaVersion"`
	CollectedAt                *string   `json:"collectedAt"`
	HeapAllocBytesTotal        *uint64   `json:"heapAllocBytesTotal"`
	HeapAllocObjectsTotal      *uint64   `json:"heapAllocObjectsTotal"`
	GCCyclesTotal              *uint64   `json:"gcCyclesTotal"`
	GCCPUSecondsTotal          *float64  `json:"gcCPUSecondsTotal"`
	GCPauseCPUSecondsTotal     *float64  `json:"gcPauseCPUSecondsTotal"`
	RuntimeCPUSecondsTotal     *float64  `json:"runtimeCPUSecondsTotal"`
	GOMAXPROCSThreads          *uint64   `json:"gomaxprocsThreads"`
	GCPauseBucketBoundsSeconds *[]string `json:"gcPauseBucketBoundsSeconds"`
	GCPauseBucketCounts        *[]uint64 `json:"gcPauseBucketCounts"`
}

type runtimeSample struct {
	collectedAt            time.Time
	heapAllocBytesTotal    uint64
	heapAllocObjectsTotal  uint64
	gcCyclesTotal          uint64
	gcCPUSecondsTotal      float64
	gcPauseCPUSecondsTotal float64
	runtimeCPUSecondsTotal float64
	gomaxprocsThreads      uint64
	pauseBounds            []float64
	pauseBoundStrings      []string
	pauseCounts            []uint64
}

type phaseBoundary struct {
	start time.Time
	end   time.Time
}

// Compute validates raw runtime metrics and aggregates cumulative deltas inside phase.
func Compute(rawPath, phasesPath, phase, target string) (Summary, error) {
	summary := Summary{
		SchemaVersion: schemaVersion,
		Target:        target,
		Applicability: "required",
		Phase:         phase,
		ReleaseGating: false,
	}
	if phase == "" {
		return invalid(summary, "phase is required")
	}
	boundary, err := readPhaseBoundary(phasesPath, phase)
	if err != nil {
		return invalid(summary, err.Error())
	}
	samples, err := readRuntimeSamples(rawPath)
	if err != nil {
		return invalid(summary, err.Error())
	}
	selected := make([]runtimeSample, 0, len(samples))
	for _, sample := range samples {
		if !sample.collectedAt.Before(boundary.start) && !sample.collectedAt.After(boundary.end) {
			selected = append(selected, sample)
		}
	}
	if len(selected) < 2 {
		return invalid(summary, fmt.Sprintf("phase %q has %d runtime samples, want at least 2", phase, len(selected)))
	}
	first := selected[0]
	last := selected[len(selected)-1]
	elapsed := last.collectedAt.Sub(first.collectedAt).Seconds()
	if elapsed <= 0 || !isFiniteNonNegative(elapsed) {
		return invalid(summary, "runtime sample timestamps do not have a positive elapsed interval")
	}

	allocatedBytes, err := counterDelta("heap allocation bytes", first.heapAllocBytesTotal, last.heapAllocBytesTotal)
	if err != nil {
		return invalid(summary, err.Error())
	}
	allocationCount, err := counterDelta("heap allocation objects", first.heapAllocObjectsTotal, last.heapAllocObjectsTotal)
	if err != nil {
		return invalid(summary, err.Error())
	}
	gcCycles, err := counterDelta("GC cycles", first.gcCyclesTotal, last.gcCyclesTotal)
	if err != nil {
		return invalid(summary, err.Error())
	}
	gcCPUSeconds, err := floatDelta("GC CPU seconds", first.gcCPUSecondsTotal, last.gcCPUSecondsTotal)
	if err != nil {
		return invalid(summary, err.Error())
	}
	gcPauseCPUSeconds, err := floatDelta("GC pause CPU seconds", first.gcPauseCPUSecondsTotal, last.gcPauseCPUSecondsTotal)
	if err != nil {
		return invalid(summary, err.Error())
	}
	runtimeCPUSeconds, err := floatDelta("runtime CPU seconds", first.runtimeCPUSecondsTotal, last.runtimeCPUSecondsTotal)
	if err != nil {
		return invalid(summary, err.Error())
	}
	cpuMetricsAvailable := runtimeCPUSeconds > 0
	gcCPUFraction := 0.0
	if cpuMetricsAvailable {
		gcCPUFraction = gcCPUSeconds / runtimeCPUSeconds
		if !isFiniteNonNegative(gcCPUFraction) || gcCPUFraction > 1 {
			return invalid(summary, fmt.Sprintf("GC CPU fraction %v is outside [0,1]", gcCPUFraction))
		}
	} else if gcCPUSeconds > 0 || gcPauseCPUSeconds > 0 {
		return invalid(summary, "GC CPU counters advanced while runtime CPU seconds delta is zero")
	}
	pauseDeltas, pauseCount, err := histogramDelta(first, last)
	if err != nil {
		return invalid(summary, err.Error())
	}
	pausePercentiles, err := pauseQuantiles(last.pauseBounds, pauseDeltas, pauseCount)
	if err != nil {
		return invalid(summary, err.Error())
	}
	gomaxprocsMin, gomaxprocsMax := selected[0].gomaxprocsThreads, selected[0].gomaxprocsThreads
	for _, sample := range selected[1:] {
		if sample.gomaxprocsThreads < gomaxprocsMin {
			gomaxprocsMin = sample.gomaxprocsThreads
		}
		if sample.gomaxprocsThreads > gomaxprocsMax {
			gomaxprocsMax = sample.gomaxprocsThreads
		}
	}
	summary.SampleCount = len(selected)
	summary.StartedAt = first.collectedAt.UTC().Format(time.RFC3339Nano)
	summary.FinishedAt = last.collectedAt.UTC().Format(time.RFC3339Nano)
	summary.ElapsedSeconds = elapsed
	summary.AllocatedBytes = allocatedBytes
	summary.AllocationCount = allocationCount
	summary.AllocationBytesPerSecond = float64(allocatedBytes) / elapsed
	summary.AllocationsPerSecond = float64(allocationCount) / elapsed
	summary.GCCycles = gcCycles
	summary.GCCPUSeconds = gcCPUSeconds
	summary.GCPauseCPUSeconds = gcPauseCPUSeconds
	summary.RuntimeCPUSeconds = runtimeCPUSeconds
	if !cpuMetricsAvailable {
		summary.CPUAvailabilityReason = "runtime CPU seconds delta was zero in the selected phase"
	}
	summary.CPUMetricsAvailable = cpuMetricsAvailable
	summary.GCCPUFraction = gcCPUFraction
	summary.GCPauseCount = pauseCount
	summary.GCPauseP50Seconds = pausePercentiles[0]
	summary.GCPauseP95Seconds = pausePercentiles[1]
	summary.GCPauseP99Seconds = pausePercentiles[2]
	summary.GCPauseP999Seconds = pausePercentiles[3]
	summary.GOMAXPROCSMin = gomaxprocsMin
	summary.GOMAXPROCSMax = gomaxprocsMax
	summary.Valid = true
	return summary, nil
}

// NotApplicable returns a valid explicit marker for targets without Go runtime metrics.
func NotApplicable(target, reason string) Summary {
	return Summary{
		SchemaVersion:       schemaVersion,
		Target:              target,
		Applicability:       "not_applicable",
		Valid:               true,
		ReleaseGating:       false,
		NotApplicableReason: reason,
	}
}

func invalid(summary Summary, reason string) (Summary, error) {
	summary.Valid = false
	summary.FailureReason = reason
	return summary, errors.New(reason)
}

func readRuntimeSamples(path string) ([]runtimeSample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open runtime metrics %q: %w", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	decoder.DisallowUnknownFields()
	var samples []runtimeSample
	var previous *runtimeSample
	for index := 0; ; index++ {
		var record runtimeRecord
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode runtime metrics sample %d: %w", index, err)
		}
		sample, err := record.sample(index)
		if err != nil {
			return nil, err
		}
		if previous != nil {
			if !sample.collectedAt.After(previous.collectedAt) {
				return nil, fmt.Errorf("runtime metrics sample %d timestamp %s is not strictly after previous sample", index, sample.collectedAt.Format(time.RFC3339Nano))
			}
			if err := validateCumulative(previous, &sample, index); err != nil {
				return nil, err
			}
		}
		samples = append(samples, sample)
		previous = &samples[len(samples)-1]
	}
	if len(samples) == 0 {
		return nil, errors.New("runtime metrics artifact contains no samples")
	}
	return samples, nil
}

func (record runtimeRecord) sample(index int) (runtimeSample, error) {
	if record.SchemaVersion == nil {
		return runtimeSample{}, fmt.Errorf("runtime metrics sample %d is missing schemaVersion", index)
	}
	if *record.SchemaVersion != schemaVersion {
		return runtimeSample{}, fmt.Errorf("runtime metrics sample %d schemaVersion = %d, want %d", index, *record.SchemaVersion, schemaVersion)
	}
	if record.CollectedAt == nil {
		return runtimeSample{}, fmt.Errorf("runtime metrics sample %d is missing collectedAt", index)
	}
	collectedAt, err := time.Parse(time.RFC3339Nano, *record.CollectedAt)
	if err != nil {
		return runtimeSample{}, fmt.Errorf("runtime metrics sample %d collectedAt: %w", index, err)
	}
	missing := func(name string) error {
		return fmt.Errorf("runtime metrics sample %d is missing %s", index, name)
	}
	if record.HeapAllocBytesTotal == nil {
		return runtimeSample{}, missing("heapAllocBytesTotal")
	}
	if record.HeapAllocObjectsTotal == nil {
		return runtimeSample{}, missing("heapAllocObjectsTotal")
	}
	if record.GCCyclesTotal == nil {
		return runtimeSample{}, missing("gcCyclesTotal")
	}
	if record.GCCPUSecondsTotal == nil {
		return runtimeSample{}, missing("gcCPUSecondsTotal")
	}
	if record.GCPauseCPUSecondsTotal == nil {
		return runtimeSample{}, missing("gcPauseCPUSecondsTotal")
	}
	if record.RuntimeCPUSecondsTotal == nil {
		return runtimeSample{}, missing("runtimeCPUSecondsTotal")
	}
	if record.GOMAXPROCSThreads == nil {
		return runtimeSample{}, missing("gomaxprocsThreads")
	}
	if record.GCPauseBucketBoundsSeconds == nil {
		return runtimeSample{}, missing("gcPauseBucketBoundsSeconds")
	}
	if record.GCPauseBucketCounts == nil {
		return runtimeSample{}, missing("gcPauseBucketCounts")
	}
	values := []float64{
		*record.GCCPUSecondsTotal,
		*record.GCPauseCPUSecondsTotal,
		*record.RuntimeCPUSecondsTotal,
	}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return runtimeSample{}, fmt.Errorf("runtime metrics sample %d contains invalid cumulative CPU value %v", index, value)
		}
	}
	if len(*record.GCPauseBucketBoundsSeconds) != len(*record.GCPauseBucketCounts)+1 {
		return runtimeSample{}, fmt.Errorf("runtime metrics sample %d histogram has %d bounds and %d counts", index, len(*record.GCPauseBucketBoundsSeconds), len(*record.GCPauseBucketCounts))
	}
	bounds := make([]float64, len(*record.GCPauseBucketBoundsSeconds))
	for boundIndex, value := range *record.GCPauseBucketBoundsSeconds {
		bound, err := parseBound(value)
		if err != nil {
			return runtimeSample{}, fmt.Errorf("runtime metrics sample %d histogram bound %d: %w", index, boundIndex, err)
		}
		bounds[boundIndex] = bound
		if boundIndex > 0 && !greaterBound(bound, bounds[boundIndex-1]) {
			return runtimeSample{}, fmt.Errorf("runtime metrics sample %d histogram bounds are not increasing at index %d", index, boundIndex)
		}
	}
	return runtimeSample{
		collectedAt:            collectedAt,
		heapAllocBytesTotal:    *record.HeapAllocBytesTotal,
		heapAllocObjectsTotal:  *record.HeapAllocObjectsTotal,
		gcCyclesTotal:          *record.GCCyclesTotal,
		gcCPUSecondsTotal:      *record.GCCPUSecondsTotal,
		gcPauseCPUSecondsTotal: *record.GCPauseCPUSecondsTotal,
		runtimeCPUSecondsTotal: *record.RuntimeCPUSecondsTotal,
		gomaxprocsThreads:      *record.GOMAXPROCSThreads,
		pauseBounds:            bounds,
		pauseBoundStrings:      append([]string(nil), (*record.GCPauseBucketBoundsSeconds)...),
		pauseCounts:            append([]uint64(nil), (*record.GCPauseBucketCounts)...),
	}, nil
}

func parseBound(value string) (float64, error) {
	switch value {
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	default:
		bound, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(bound) || math.IsInf(bound, 0) {
			return 0, fmt.Errorf("invalid bound %q", value)
		}
		return bound, nil
	}
}

func greaterBound(left, right float64) bool {
	if math.IsInf(left, 1) {
		return !math.IsInf(right, 1)
	}
	if math.IsInf(right, -1) {
		return !math.IsInf(left, -1)
	}
	return left > right
}

func validateCumulative(previous, current *runtimeSample, index int) error {
	if current.heapAllocBytesTotal < previous.heapAllocBytesTotal {
		return fmt.Errorf("runtime metrics sample %d heap allocation bytes counter decreased", index)
	}
	if current.heapAllocObjectsTotal < previous.heapAllocObjectsTotal {
		return fmt.Errorf("runtime metrics sample %d heap allocation objects counter decreased", index)
	}
	if current.gcCyclesTotal < previous.gcCyclesTotal {
		return fmt.Errorf("runtime metrics sample %d GC cycles counter decreased", index)
	}
	if current.gcCPUSecondsTotal < previous.gcCPUSecondsTotal || current.gcPauseCPUSecondsTotal < previous.gcPauseCPUSecondsTotal || current.runtimeCPUSecondsTotal < previous.runtimeCPUSecondsTotal {
		return fmt.Errorf("runtime metrics sample %d CPU counter decreased", index)
	}
	if !reflect.DeepEqual(current.pauseBoundStrings, previous.pauseBoundStrings) {
		return fmt.Errorf("runtime metrics sample %d histogram bucket bounds changed", index)
	}
	for bucket, count := range current.pauseCounts {
		if count < previous.pauseCounts[bucket] {
			return fmt.Errorf("runtime metrics sample %d histogram bucket %d count decreased", index, bucket)
		}
	}
	return nil
}

func readPhaseBoundary(path, phase string) (phaseBoundary, error) {
	file, err := os.Open(path)
	if err != nil {
		return phaseBoundary{}, fmt.Errorf("open phase artifact %q: %w", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	var boundary phaseBoundary
	starts, ends := 0, 0
	for index := 0; ; index++ {
		var record struct {
			Phase     *string `json:"phase"`
			Event     *string `json:"event"`
			Timestamp *string `json:"timestamp"`
		}
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return phaseBoundary{}, fmt.Errorf("decode phase event %d: %w", index, err)
		}
		if record.Phase == nil || record.Event == nil || record.Timestamp == nil {
			return phaseBoundary{}, fmt.Errorf("phase event %d is missing phase, event, or timestamp", index)
		}
		if *record.Phase != phase || (*record.Event != "start" && *record.Event != "end") {
			continue
		}
		timestamp, err := time.Parse(time.RFC3339Nano, *record.Timestamp)
		if err != nil {
			return phaseBoundary{}, fmt.Errorf("phase event %d timestamp: %w", index, err)
		}
		switch *record.Event {
		case "start":
			starts++
			boundary.start = timestamp
		case "end":
			ends++
			boundary.end = timestamp
		}
	}
	if starts != 1 || ends != 1 {
		return phaseBoundary{}, fmt.Errorf("phase %q requires exactly one start and one end, got %d start and %d end", phase, starts, ends)
	}
	if boundary.start.After(boundary.end) {
		return phaseBoundary{}, fmt.Errorf("phase %q start is after end", phase)
	}
	return boundary, nil
}

func counterDelta(name string, first, last uint64) (uint64, error) {
	if last < first {
		return 0, fmt.Errorf("%s counter decreased", name)
	}
	return last - first, nil
}

func floatDelta(name string, first, last float64) (float64, error) {
	if last < first {
		return 0, fmt.Errorf("%s counter decreased", name)
	}
	delta := last - first
	if !isFiniteNonNegative(delta) {
		return 0, fmt.Errorf("%s delta is invalid: %v", name, delta)
	}
	return delta, nil
}

func histogramDelta(first, last runtimeSample) ([]uint64, uint64, error) {
	if !reflect.DeepEqual(first.pauseBoundStrings, last.pauseBoundStrings) {
		return nil, 0, errors.New("runtime GC pause histogram bucket bounds changed")
	}
	deltas := make([]uint64, len(first.pauseCounts))
	var total uint64
	for index, count := range last.pauseCounts {
		if count < first.pauseCounts[index] {
			return nil, 0, fmt.Errorf("runtime GC pause histogram bucket %d count decreased", index)
		}
		delta := count - first.pauseCounts[index]
		deltas[index] = delta
		if ^uint64(0)-total < delta {
			return nil, 0, errors.New("runtime GC pause histogram count overflow")
		}
		total += delta
	}
	return deltas, total, nil
}

func pauseQuantiles(bounds []float64, deltas []uint64, total uint64) ([4]float64, error) {
	var quantiles [4]float64
	if total == 0 {
		return quantiles, nil
	}
	percentiles := [...]float64{0.50, 0.95, 0.99, 0.999}
	for index, percentile := range percentiles {
		rank := uint64(math.Ceil(percentile * float64(total)))
		if rank == 0 {
			rank = 1
		}
		var cumulative uint64
		selected := -1
		for bucket, count := range deltas {
			cumulative += count
			if cumulative >= rank {
				selected = bucket
				break
			}
		}
		if selected < 0 {
			return quantiles, errors.New("runtime GC pause histogram quantile rank exceeded count")
		}
		upper := bounds[selected+1]
		if math.IsInf(upper, 1) || upper < 0 {
			return quantiles, fmt.Errorf("runtime GC pause quantile bucket %d has invalid upper bound %v", selected, upper)
		}
		quantiles[index] = upper
	}
	return quantiles, nil
}

func isFiniteNonNegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}
