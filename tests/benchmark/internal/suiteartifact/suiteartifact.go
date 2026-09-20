// Package suiteartifact owns benchmark repetition ordering and robust suite statistics.
package suiteartifact

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// MinimumRepetitions is the minimum independent executions required by PERF-07.
const MinimumRepetitions = 5

// MetricSample is one finite metric observation from one suite repetition.
type MetricSample struct {
	Repetition int     `json:"repetition"`
	RunID      string  `json:"runId"`
	Value      float64 `json:"value"`
}

// MetricSummary contains the robust center and spread of metric observations.
type MetricSummary struct {
	Unit                    string         `json:"unit"`
	SampleCount             int            `json:"sampleCount"`
	Samples                 []MetricSample `json:"samples"`
	Median                  float64        `json:"median"`
	MedianAbsoluteDeviation float64        `json:"medianAbsoluteDeviation"`
	Min                     float64        `json:"min"`
	Max                     float64        `json:"max"`
}

// AlternatingTargetOrder returns the deterministic target order for one repetition.
func AlternatingTargetOrder(repetition int) ([]string, error) {
	if repetition < 1 {
		return nil, errors.New("repetition must be at least 1")
	}
	if repetition%2 == 1 {
		return []string{"narumq", "emqx"}, nil
	}
	return []string{"emqx", "narumq"}, nil
}

// SummarizeMetric validates samples and computes median and median absolute deviation.
func SummarizeMetric(unit string, samples []MetricSample) (MetricSummary, error) {
	if strings.TrimSpace(unit) == "" {
		return MetricSummary{}, errors.New("metric unit is required")
	}
	if len(samples) == 0 {
		return MetricSummary{}, errors.New("metric samples are required")
	}

	copySamples := append([]MetricSample(nil), samples...)
	values := make([]float64, len(copySamples))
	seenRepetitions := make(map[int]struct{}, len(copySamples))
	seenRunIDs := make(map[string]struct{}, len(copySamples))
	for index, sample := range copySamples {
		if sample.Repetition < 1 {
			return MetricSummary{}, fmt.Errorf("sample %d has invalid repetition %d", index, sample.Repetition)
		}
		if _, exists := seenRepetitions[sample.Repetition]; exists {
			return MetricSummary{}, fmt.Errorf("duplicate repetition %d", sample.Repetition)
		}
		seenRepetitions[sample.Repetition] = struct{}{}
		if strings.TrimSpace(sample.RunID) == "" {
			return MetricSummary{}, fmt.Errorf("sample %d has an empty run ID", index)
		}
		if _, exists := seenRunIDs[sample.RunID]; exists {
			return MetricSummary{}, fmt.Errorf("duplicate run ID %q", sample.RunID)
		}
		seenRunIDs[sample.RunID] = struct{}{}
		if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
			return MetricSummary{}, fmt.Errorf("sample %d has a non-finite value", index)
		}
		values[index] = sample.Value
	}

	sortedValues := append([]float64(nil), values...)
	sort.Float64s(sortedValues)
	medianValue := median(sortedValues)
	deviations := make([]float64, len(sortedValues))
	for index, value := range sortedValues {
		deviations[index] = math.Abs(value - medianValue)
	}
	sort.Float64s(deviations)
	mad := median(deviations)
	if math.IsNaN(medianValue) || math.IsInf(medianValue, 0) || math.IsNaN(mad) || math.IsInf(mad, 0) {
		return MetricSummary{}, errors.New("metric statistics are non-finite")
	}

	return MetricSummary{
		Unit:                    unit,
		SampleCount:             len(copySamples),
		Samples:                 copySamples,
		Median:                  medianValue,
		MedianAbsoluteDeviation: mad,
		Min:                     sortedValues[0],
		Max:                     sortedValues[len(sortedValues)-1],
	}, nil
}

func median(sorted []float64) float64 {
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}
