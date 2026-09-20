package suiteartifact

import (
	"math"
	"reflect"
	"testing"
)

func TestAlternatingTargetOrder(t *testing.T) {
	want := [][]string{
		{"narumq", "emqx"},
		{"emqx", "narumq"},
		{"narumq", "emqx"},
		{"emqx", "narumq"},
		{"narumq", "emqx"},
	}
	for repetition, expected := range want {
		got, err := AlternatingTargetOrder(repetition + 1)
		if err != nil {
			t.Fatalf("AlternatingTargetOrder(%d) error = %v", repetition+1, err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("AlternatingTargetOrder(%d) = %#v, want %#v", repetition+1, got, expected)
		}
	}
	if _, err := AlternatingTargetOrder(0); err == nil {
		t.Fatal("AlternatingTargetOrder(0) accepted invalid repetition")
	}
}

func TestSummarizeMetricComputesMedianAndMADWithoutMutatingInput(t *testing.T) {
	samples := []MetricSample{
		{Repetition: 1, RunID: "run-1", Value: 1},
		{Repetition: 2, RunID: "run-2", Value: 10},
		{Repetition: 3, RunID: "run-3", Value: 2},
		{Repetition: 4, RunID: "run-4", Value: 8},
		{Repetition: 5, RunID: "run-5", Value: 4},
	}
	original := append([]MetricSample(nil), samples...)
	got, err := SummarizeMetric("count/second", samples)
	if err != nil {
		t.Fatalf("SummarizeMetric() error = %v", err)
	}
	if !reflect.DeepEqual(samples, original) {
		t.Fatalf("SummarizeMetric() mutated input: got %#v, want %#v", samples, original)
	}
	if got.Unit != "count/second" || got.SampleCount != 5 || got.Median != 4 || got.MedianAbsoluteDeviation != 3 || got.Min != 1 || got.Max != 10 {
		t.Fatalf("SummarizeMetric() = %#v, want median 4 MAD 3 range 1..10", got)
	}
	got, err = SummarizeMetric("nanoseconds", []MetricSample{
		{Repetition: 1, RunID: "run-1", Value: 1},
		{Repetition: 2, RunID: "run-2", Value: 3},
		{Repetition: 3, RunID: "run-3", Value: 5},
		{Repetition: 4, RunID: "run-4", Value: 7},
	})
	if err != nil {
		t.Fatalf("SummarizeMetric(even) error = %v", err)
	}
	if got.Median != 4 || got.MedianAbsoluteDeviation != 2 {
		t.Fatalf("SummarizeMetric(even) = %#v, want median 4 MAD 2", got)
	}
}

func TestSummarizeMetricRejectsInvalidSamples(t *testing.T) {
	cases := []struct {
		name    string
		unit    string
		samples []MetricSample
	}{
		{name: "empty unit", unit: " ", samples: []MetricSample{{Repetition: 1, RunID: "run", Value: 1}}},
		{name: "empty samples", unit: "count/second"},
		{name: "invalid repetition", unit: "count/second", samples: []MetricSample{{Repetition: 0, RunID: "run", Value: 1}}},
		{name: "duplicate repetition", unit: "count/second", samples: []MetricSample{{Repetition: 1, RunID: "a", Value: 1}, {Repetition: 1, RunID: "b", Value: 2}}},
		{name: "empty run ID", unit: "count/second", samples: []MetricSample{{Repetition: 1, RunID: " ", Value: 1}}},
		{name: "duplicate run ID", unit: "count/second", samples: []MetricSample{{Repetition: 1, RunID: "run", Value: 1}, {Repetition: 2, RunID: "run", Value: 2}}},
		{name: "NaN", unit: "count/second", samples: []MetricSample{{Repetition: 1, RunID: "run", Value: math.NaN()}}},
		{name: "positive infinity", unit: "count/second", samples: []MetricSample{{Repetition: 1, RunID: "run", Value: math.Inf(1)}}},
		{name: "negative infinity", unit: "count/second", samples: []MetricSample{{Repetition: 1, RunID: "run", Value: math.Inf(-1)}}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := SummarizeMetric(testCase.unit, testCase.samples); err == nil {
				t.Fatal("SummarizeMetric() accepted invalid input")
			}
		})
	}
}
