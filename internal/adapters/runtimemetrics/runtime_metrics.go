package runtimemetrics

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"
)

const schemaVersion = 1

type metricSpec struct {
	name string
	kind metrics.ValueKind
}

var metricSpecs = [...]metricSpec{
	{name: "/gc/heap/allocs:bytes", kind: metrics.KindUint64},
	{name: "/gc/heap/allocs:objects", kind: metrics.KindUint64},
	{name: "/gc/cycles/total:gc-cycles", kind: metrics.KindUint64},
	{name: "/cpu/classes/gc/total:cpu-seconds", kind: metrics.KindFloat64},
	{name: "/cpu/classes/gc/pause:cpu-seconds", kind: metrics.KindFloat64},
	{name: "/cpu/classes/total:cpu-seconds", kind: metrics.KindFloat64},
	{name: "/sched/gomaxprocs:threads", kind: metrics.KindUint64},
	{name: "/sched/pauses/total/gc:seconds", kind: metrics.KindFloat64Histogram},
}

type runtimeMetricsRecord struct {
	SchemaVersion              int      `json:"schemaVersion"`
	CollectedAt                string   `json:"collectedAt"`
	HeapAllocBytesTotal        uint64   `json:"heapAllocBytesTotal"`
	HeapAllocObjectsTotal      uint64   `json:"heapAllocObjectsTotal"`
	GCCyclesTotal              uint64   `json:"gcCyclesTotal"`
	GCCPUSecondsTotal          float64  `json:"gcCPUSecondsTotal"`
	GCPauseCPUSecondsTotal     float64  `json:"gcPauseCPUSecondsTotal"`
	RuntimeCPUSecondsTotal     float64  `json:"runtimeCPUSecondsTotal"`
	GOMAXPROCSThreads          uint64   `json:"gomaxprocsThreads"`
	GCPauseBucketBoundsSeconds []string `json:"gcPauseBucketBoundsSeconds"`
	GCPauseBucketCounts        []uint64 `json:"gcPauseBucketCounts"`
}

// Sampler records selected Go runtime/metrics values as JSONL rows.
type Sampler struct {
	mu              sync.Mutex
	file            *os.File
	writer          *bufio.Writer
	samples         []metrics.Sample
	interval        time.Duration
	started         bool
	lastCollectedAt time.Time
}

// New creates a truncated runtime metrics output and validates the metric contract.
func New(outputPath string, interval time.Duration) (*Sampler, error) {
	if strings.TrimSpace(outputPath) == "" {
		return nil, errors.New("runtime metrics output path is required")
	}
	if interval <= 0 {
		return nil, errors.New("runtime metrics interval must be greater than zero")
	}
	if err := validateMetricCatalog(); err != nil {
		return nil, err
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("create runtime metrics output: %w", err)
	}
	samples := make([]metrics.Sample, len(metricSpecs))
	for index, spec := range metricSpecs {
		samples[index].Name = spec.name
	}
	return &Sampler{
		file:     file,
		writer:   bufio.NewWriterSize(file, 64*1024),
		samples:  samples,
		interval: interval,
	}, nil
}

// Run records an immediate sample, samples on absolute deadlines, and closes on cancellation.
func (s *Sampler) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil {
		return errors.New("runtime metrics sampler is nil")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("runtime metrics sampler can only run once")
	}
	s.started = true
	s.mu.Unlock()

	finish := func(sampleErr error) error {
		closeErr := s.close()
		if sampleErr == nil {
			return closeErr
		}
		if closeErr == nil {
			return sampleErr
		}
		return errors.Join(sampleErr, closeErr)
	}
	if err := s.writeSample(time.Now()); err != nil {
		return finish(fmt.Errorf("write initial runtime metrics sample: %w", err))
	}

	startedAt := time.Now()
	nextDeadline := startedAt.Add(s.interval)
	for {
		delay := time.Until(nextDeadline)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if err := s.writeSample(time.Now()); err != nil {
				return finish(fmt.Errorf("write final runtime metrics sample: %w", err))
			}
			return finish(nil)
		case <-timer.C:
			if err := s.writeSample(time.Now()); err != nil {
				return finish(fmt.Errorf("write runtime metrics sample: %w", err))
			}
			nextDeadline = nextDeadline.Add(s.interval)
		}
	}
}

func validateMetricCatalog() error {
	available := make(map[string]metrics.ValueKind, len(metrics.All()))
	for _, description := range metrics.All() {
		available[description.Name] = description.Kind
	}
	for _, spec := range metricSpecs {
		kind, ok := available[spec.name]
		if !ok {
			return fmt.Errorf("required runtime metric %q is unavailable", spec.name)
		}
		if kind != spec.kind {
			return fmt.Errorf("runtime metric %q has kind %v, want %v", spec.name, kind, spec.kind)
		}
	}
	return nil
}

func (s *Sampler) writeSample(collectedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.writer == nil {
		return errors.New("runtime metrics sampler is closed")
	}
	if !collectedAt.After(s.lastCollectedAt) {
		return nil
	}
	s.lastCollectedAt = collectedAt
	metrics.Read(s.samples)
	record := runtimeMetricsRecord{
		SchemaVersion:          schemaVersion,
		CollectedAt:            collectedAt.UTC().Format(time.RFC3339Nano),
		HeapAllocBytesTotal:    s.samples[0].Value.Uint64(),
		HeapAllocObjectsTotal:  s.samples[1].Value.Uint64(),
		GCCyclesTotal:          s.samples[2].Value.Uint64(),
		GCCPUSecondsTotal:      s.samples[3].Value.Float64(),
		GCPauseCPUSecondsTotal: s.samples[4].Value.Float64(),
		RuntimeCPUSecondsTotal: s.samples[5].Value.Float64(),
		GOMAXPROCSThreads:      s.samples[6].Value.Uint64(),
	}
	histogram := s.samples[7].Value.Float64Histogram()
	if histogram == nil {
		return errors.New("runtime GC pause histogram is nil")
	}
	if len(histogram.Buckets) != len(histogram.Counts)+1 {
		return fmt.Errorf("runtime GC pause histogram has %d bounds and %d counts", len(histogram.Buckets), len(histogram.Counts))
	}
	record.GCPauseBucketBoundsSeconds = make([]string, len(histogram.Buckets))
	for index, bound := range histogram.Buckets {
		switch {
		case math.IsInf(bound, 1):
			record.GCPauseBucketBoundsSeconds[index] = "+Inf"
		case math.IsInf(bound, -1):
			record.GCPauseBucketBoundsSeconds[index] = "-Inf"
		default:
			record.GCPauseBucketBoundsSeconds[index] = strconv.FormatFloat(bound, 'g', -1, 64)
		}
	}
	record.GCPauseBucketCounts = append([]uint64(nil), histogram.Counts...)
	if err := json.NewEncoder(s.writer).Encode(record); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *Sampler) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	flushErr := s.writer.Flush()
	closeErr := s.file.Close()
	s.file = nil
	s.writer = nil
	s.samples = nil
	if flushErr != nil && closeErr != nil {
		return errors.Join(fmt.Errorf("flush runtime metrics output: %w", flushErr), fmt.Errorf("close runtime metrics output: %w", closeErr))
	}
	if flushErr != nil {
		return fmt.Errorf("flush runtime metrics output: %w", flushErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close runtime metrics output: %w", closeErr)
	}
	return nil
}
