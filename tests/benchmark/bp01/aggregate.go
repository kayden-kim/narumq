package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	phaseWarmup      = "warmup"
	phaseMeasurement = "measure"
)

// Sample is one delivery observation written immediately to samples.jsonl.
type Sample struct {
	Sequence               uint64 `json:"sequence"`
	Phase                  string `json:"phase"`
	ScheduledAt            string `json:"scheduledAt"`
	SentAt                 string `json:"sentAt"`
	ReceivedAt             string `json:"receivedAt"`
	ScheduleLagNanoseconds int64  `json:"scheduleLagNanoseconds"`
	LatencyNanoseconds     int64  `json:"latencyNanoseconds"`
}

// Percentiles contains nearest-rank latency values in nanoseconds.
type Percentiles struct {
	P50Nanoseconds  int64 `json:"p50Nanoseconds"`
	P95Nanoseconds  int64 `json:"p95Nanoseconds"`
	P99Nanoseconds  int64 `json:"p99Nanoseconds"`
	P999Nanoseconds int64 `json:"p999Nanoseconds"`
}

// Summary is the stable aggregate result for one target run.
type Summary struct {
	SchemaVersion             int         `json:"schemaVersion"`
	Target                    string      `json:"target"`
	ProfileID                 string      `json:"profileId"`
	Purpose                   string      `json:"purpose"`
	Offered                   uint64      `json:"offered"`
	Sent                      uint64      `json:"sent"`
	Delivered                 uint64      `json:"delivered"`
	PublishRatePerSecond      float64     `json:"publishRatePerSecond"`
	DeliveryRatePerSecond     float64     `json:"deliveryRatePerSecond"`
	LatencyNanoseconds        Percentiles `json:"latencyNanoseconds"`
	MaxScheduleLagNanoseconds int64       `json:"maxScheduleLagNanoseconds"`
	Duplicate                 uint64      `json:"duplicate"`
	Missing                   uint64      `json:"missing"`
	OutOfRange                uint64      `json:"outOfRange"`
	ProtocolErrors            uint64      `json:"protocolErrors"`
	GeneratorOverload         uint64      `json:"generatorOverload"`
	ErrorCount                uint64      `json:"errorCount"`
	Valid                     bool        `json:"valid"`
	ReleaseGating             bool        `json:"releaseGating"`
}

type sentRecord struct {
	phase       string
	scheduledAt time.Time
	sentAt      time.Time
	scheduleLag time.Duration
	pending     bool
	aborted     bool
	overloaded  bool
	delivered   bool
}

// Collector owns the bounded delivery state and the samples JSONL writer.
type Collector struct {
	mu sync.Mutex

	profile Profile
	target  string
	writer  *bufio.Writer
	file    *os.File

	offered           uint64
	sent              uint64
	delivered         uint64
	duplicate         uint64
	missing           uint64
	outOfRange        uint64
	protocolErrors    uint64
	generatorOverload uint64
	maxScheduleLag    time.Duration
	latencies         []int64
	sentBySequence    map[uint64]sentRecord
}

// NewCollector creates a collector and its line-buffered raw sample file.
func NewCollector(profile Profile, target string, outputDir string) (*Collector, error) {
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	if target != "narumq" && target != "emqx" {
		return nil, fmt.Errorf("unsupported benchmark target %q", target)
	}
	if outputDir == "" {
		return nil, errors.New("output directory is required")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	file, err := os.Create(outputDir + string(os.PathSeparator) + "samples.jsonl")
	if err != nil {
		return nil, fmt.Errorf("create samples.jsonl: %w", err)
	}
	return &Collector{
		profile:        profile,
		target:         target,
		writer:         bufio.NewWriterSize(file, 64*1024),
		file:           file,
		sentBySequence: make(map[uint64]sentRecord),
		latencies:      make([]int64, 0, minUint64(profile.MaxSamples, uint64(math.MaxInt))),
	}, nil
}

func minUint64(left, right uint64) int {
	if left < right {
		return int(left)
	}
	return int(right)
}

// SetOffered records the number of measurement messages the scheduler intends
// to send before the first packet is written.
func (c *Collector) SetOffered(offered uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offered = offered
}

// RecordSent stores one scheduled sequence and emits no raw line yet. The
// sequence map is bounded by the profile's workload and is guarded for the
// receiver goroutine.
func (c *Collector) RecordSent(sequence uint64, phase string, scheduledAt, sentAt time.Time, scheduleLag time.Duration, overloaded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := sentRecord{
		phase:       phase,
		scheduledAt: scheduledAt,
		sentAt:      sentAt,
		scheduleLag: scheduleLag,
		overloaded:  overloaded,
	}
	c.sentBySequence[sequence] = record
	c.commitSentLocked(record)
}

// WriteAndRecord registers a sequence before invoking write. A successful
// write commits the send counters. A failed write retains an uncertain
// sequence until finalization so a late delivery can still commit it.
func (c *Collector) WriteAndRecord(write func() error, sequence uint64, phase string, scheduledAt, sentAt time.Time, scheduleLag time.Duration, overloaded bool) error {
	c.mu.Lock()
	c.sentBySequence[sequence] = sentRecord{
		phase:       phase,
		scheduledAt: scheduledAt,
		sentAt:      sentAt,
		scheduleLag: scheduleLag,
		pending:     true,
		overloaded:  overloaded,
	}
	c.mu.Unlock()

	if err := write(); err != nil {
		c.mu.Lock()
		if record, ok := c.sentBySequence[sequence]; ok && record.pending {
			record.pending = false
			if record.delivered {
				c.sentBySequence[sequence] = record
				c.commitSentLocked(record)
			} else {
				record.aborted = true
				c.sentBySequence[sequence] = record
			}
		}
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.sentBySequence[sequence]
	if !ok || !record.pending {
		return fmt.Errorf("sequence %d is not pending", sequence)
	}
	record.pending = false
	c.sentBySequence[sequence] = record
	c.commitSentLocked(record)
	return nil
}

func (c *Collector) commitSentLocked(record sentRecord) {
	if record.phase != phaseMeasurement {
		return
	}
	c.sent++
	if record.scheduleLag > c.maxScheduleLag {
		c.maxScheduleLag = record.scheduleLag
	}
	if record.overloaded {
		c.generatorOverload++
	}
}

// RecordDelivery records a valid payload delivery and writes its raw sample.
// It returns an error only when the received timestamp precedes the send time.
func (c *Collector) RecordDelivery(sequence uint64, receivedAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.sentBySequence[sequence]
	if !ok {
		c.outOfRange++
		return c.writeSampleLocked(Sample{
			Sequence:   sequence,
			Phase:      "out_of_range",
			ReceivedAt: receivedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	latency := receivedAt.Sub(record.sentAt)
	if latency < 0 {
		c.protocolErrors++
		return errors.New("delivery timestamp precedes publish timestamp")
	}
	if record.aborted {
		record.aborted = false
		c.sentBySequence[sequence] = record
		c.commitSentLocked(record)
	}
	if record.delivered {
		c.duplicate++
	} else {
		record.delivered = true
		c.sentBySequence[sequence] = record
		if record.phase == phaseMeasurement {
			c.delivered++
			c.latencies = append(c.latencies, latency.Nanoseconds())
		}
	}
	return c.writeSampleLocked(Sample{
		Sequence:               sequence,
		Phase:                  record.phase,
		ScheduledAt:            record.scheduledAt.UTC().Format(time.RFC3339Nano),
		SentAt:                 record.sentAt.UTC().Format(time.RFC3339Nano),
		ReceivedAt:             receivedAt.UTC().Format(time.RFC3339Nano),
		ScheduleLagNanoseconds: record.scheduleLag.Nanoseconds(),
		LatencyNanoseconds:     latency.Nanoseconds(),
	})
}

// RecordProtocolError adds one protocol or transport error to the summary.
func (c *Collector) RecordProtocolError() {
	c.mu.Lock()
	c.protocolErrors++
	c.mu.Unlock()
}

// Finalize flushes raw samples, counts missing measurement sequences, and
// returns the deterministic summary. It must be called once by the owner.
func (c *Collector) Finalize() (Summary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range c.sentBySequence {
		if record.phase == phaseMeasurement && !record.aborted && !record.delivered {
			c.missing++
		}
	}
	if err := c.writer.Flush(); err != nil {
		return Summary{}, fmt.Errorf("flush samples.jsonl: %w", err)
	}
	if err := c.file.Close(); err != nil {
		return Summary{}, fmt.Errorf("close samples.jsonl: %w", err)
	}
	seconds := float64(c.profile.MeasureMillis) / 1000
	summary := Summary{
		SchemaVersion:             1,
		Target:                    c.target,
		ProfileID:                 c.profile.ProfileID,
		Purpose:                   c.profile.Purpose,
		Offered:                   c.offered,
		Sent:                      c.sent,
		Delivered:                 c.delivered,
		PublishRatePerSecond:      float64(c.sent) / seconds,
		DeliveryRatePerSecond:     float64(c.delivered) / seconds,
		LatencyNanoseconds:        nearestRankPercentiles(c.latencies),
		MaxScheduleLagNanoseconds: c.maxScheduleLag.Nanoseconds(),
		Duplicate:                 c.duplicate,
		Missing:                   c.missing,
		OutOfRange:                c.outOfRange,
		ProtocolErrors:            c.protocolErrors,
		GeneratorOverload:         c.generatorOverload,
		Valid:                     c.sent == c.offered && c.sent == c.delivered && c.duplicate == 0 && c.missing == 0 && c.outOfRange == 0 && c.protocolErrors == 0 && c.generatorOverload == 0,
		ReleaseGating:             false,
	}
	summary.ErrorCount = summary.Duplicate + summary.Missing + summary.OutOfRange + summary.ProtocolErrors + summary.GeneratorOverload
	return summary, nil
}

func (c *Collector) writeSampleLocked(sample Sample) error {
	if err := json.NewEncoder(c.writer).Encode(sample); err != nil {
		return fmt.Errorf("encode delivery sample: %w", err)
	}
	return c.writer.Flush()
}

func nearestRankPercentiles(values []int64) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	return Percentiles{
		P50Nanoseconds:  nearestRank(sorted, 0.50),
		P95Nanoseconds:  nearestRank(sorted, 0.95),
		P99Nanoseconds:  nearestRank(sorted, 0.99),
		P999Nanoseconds: nearestRank(sorted, 0.999),
	}
}

func nearestRank(sorted []int64, percentile float64) int64 {
	rank := int(math.Ceil(percentile * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// WriteSummary stores the aggregate JSON with stable indentation.
func WriteSummary(outputDir string, summary Summary) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("encode summary: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(outputDir+string(os.PathSeparator)+"summary.json", data, 0o644); err != nil {
		return fmt.Errorf("write summary.json: %w", err)
	}
	return nil
}
