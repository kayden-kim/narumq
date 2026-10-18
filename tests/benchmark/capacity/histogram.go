package main

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// The histogram keeps memory bounded regardless of sample count. Latencies below
// histLinearMicros use exact 1 microsecond buckets, so values up to 131 ms (which includes the
// 100 ms p99 limit) are exact to 1 microsecond. Larger values use 64 sub-buckets per octave
// (<= 1.6% relative width); quantiles there report the bucket upper edge.
const (
	histLinearBits   = 17
	histLinearMicros = 1 << histLinearBits
	histSubBits      = 6
	histSubBuckets   = 1 << histSubBits
	histOctaves      = 20
	histBuckets      = histLinearMicros + histOctaves*histSubBuckets
)

type histogram struct {
	counts [histBuckets]atomic.Uint64
	total  atomic.Uint64
	maxNs  atomic.Int64
}

func newHistogram() *histogram { return &histogram{} }

func histogramIndex(micros int64) int {
	if micros < histLinearMicros {
		return int(micros)
	}
	octave := bits.Len64(uint64(micros)) - 1 - histLinearBits
	if octave >= histOctaves {
		return histBuckets - 1
	}
	sub := int(micros>>(octave+histLinearBits-histSubBits)) & (histSubBuckets - 1)
	return histLinearMicros + octave*histSubBuckets + sub
}

// histogramValue returns the reported value of a bucket in nanoseconds.
func histogramValue(index int) int64 {
	if index < histLinearMicros {
		return int64(index) * int64(time.Microsecond)
	}
	octave := (index - histLinearMicros) / histSubBuckets
	sub := (index - histLinearMicros) % histSubBuckets
	upper := int64(histSubBuckets+sub+1) << (octave + histLinearBits - histSubBits)
	return upper * int64(time.Microsecond)
}

// Record adds one latency sample; negative values are clamped to zero.
func (h *histogram) Record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.counts[histogramIndex(int64(d)/int64(time.Microsecond))].Add(1)
	h.total.Add(1)
	for {
		current := h.maxNs.Load()
		if int64(d) <= current || h.maxNs.CompareAndSwap(current, int64(d)) {
			return
		}
	}
}

func (h *histogram) Count() uint64 { return h.total.Load() }

// Quantile returns the nearest-rank quantile (q in (0,1]) or zero without samples.
func (h *histogram) Quantile(q float64) time.Duration {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(total)))
	rank = min(max(rank, 1), total)
	var seen uint64
	for index := range h.counts {
		seen += h.counts[index].Load()
		if seen >= rank {
			value := histogramValue(index)
			if maxNs := h.maxNs.Load(); value > maxNs && index >= histLinearMicros {
				value = maxNs
			}
			return time.Duration(value)
		}
	}
	return time.Duration(h.maxNs.Load())
}

func (h *histogram) Max() time.Duration { return time.Duration(h.maxNs.Load()) }
