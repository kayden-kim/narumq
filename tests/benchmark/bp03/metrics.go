package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	walDurationFamily = "narumq_raft_wal_sync_duration_seconds"
	walEntriesFamily  = "narumq_raft_wal_sync_entries"
	walBytesFamily    = "narumq_raft_wal_written_bytes_total"
)

type walNodeInput struct {
	Name       string
	Start, End []byte
}

type walWindowInput struct {
	Name          string
	Duration      time.Duration
	AckedMessages uint64
	Nodes         []walNodeInput
}

type WALSummary struct {
	SchemaVersion  int                `json:"schemaVersion"`
	Target         string             `json:"target"`
	Applicability  string             `json:"applicability,omitempty"`
	Reason         string             `json:"reason,omitempty"`
	Windows        []walWindowSummary `json:"windows"`
	Valid          bool               `json:"valid"`
	FailureReasons []string           `json:"failureReasons"`
}

type walWindowSummary struct {
	Name            string           `json:"name"`
	DurationSeconds float64          `json:"durationSeconds"`
	Nodes           []walNodeSummary `json:"nodes"`
	Cluster         walMetricSummary `json:"cluster"`
}

type walNodeSummary struct {
	Name    string           `json:"name"`
	Metrics walMetricSummary `json:"metrics"`
}

type walMetricSummary struct {
	SyncCount               uint64               `json:"syncCount"`
	SyncsPerSecond          float64              `json:"syncsPerSecond"`
	EntriesSum              uint64               `json:"entriesSum"`
	MeanEntriesPerSync      float64              `json:"meanEntriesPerSync"`
	EntriesPerSync          walEntryQuantiles    `json:"entriesPerSync"`
	SyncDurationSeconds     walDurationQuantiles `json:"syncDurationSeconds"`
	WALBytes                uint64               `json:"walBytes"`
	WALBytesPerAckedMessage *float64             `json:"walBytesPerAckedMessage"`
}

type walEntryQuantiles struct {
	P50 walQuantile `json:"p50"`
	P99 walQuantile `json:"p99"`
}

type walDurationQuantiles struct {
	P50  walQuantile `json:"p50"`
	P99  walQuantile `json:"p99"`
	P999 walQuantile `json:"p999"`
}

type walQuantile struct {
	UpperBound *float64 `json:"upperBound"`
	Overflow   bool     `json:"overflow"`
}

type walHistogram struct {
	Buckets map[float64]uint64
	Count   uint64
	Sum     uint64
}

type walScrape struct {
	Duration walHistogram
	Entries  walHistogram
	Bytes    uint64
}

func writeWALSummary(outputDir, target string, windows []walWindowInput) (WALSummary, error) {
	if target != "narumq" && target != "emqx" {
		return WALSummary{}, fmt.Errorf("invalid WAL target %q", target)
	}
	summary := WALSummary{SchemaVersion: 1, Target: target, Windows: []walWindowSummary{}, FailureReasons: []string{}}
	if target == "emqx" {
		summary.Applicability = "not_applicable"
		summary.Reason = "EMQX Durable Storage does not expose the NaruMQ Ready WAL metrics collected by this harness"
	} else {
		summary.Valid = true
		if len(windows) == 0 {
			summary.Valid = false
			summary.FailureReasons = append(summary.FailureReasons, "no WAL metric windows recorded")
		}
		for _, window := range windows {
			result := walWindowSummary{Name: window.Name, Nodes: []walNodeSummary{}}
			if window.Duration > 0 {
				result.DurationSeconds = window.Duration.Seconds()
			} else {
				summary.Valid = false
				summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("%s: duration must be positive", window.Name))
			}
			total := walMetricSummary{}
			total.WALBytesPerAckedMessage = bytesPerMessage(0, window.AckedMessages)
			for _, node := range window.Nodes {
				parsedStart, startErr := parseWALScrape(node.Start)
				parsedEnd, endErr := parseWALScrape(node.End)
				if startErr != nil {
					summary.Valid = false
					summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("%s/%s start: %v", window.Name, node.Name, startErr))
				}
				if endErr != nil {
					summary.Valid = false
					summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("%s/%s end: %v", window.Name, node.Name, endErr))
				}
				metrics := walMetricSummary{WALBytesPerAckedMessage: bytesPerMessage(0, window.AckedMessages)}
				if startErr == nil && endErr == nil {
					if parsedStart.Duration.Count != parsedStart.Entries.Count || parsedEnd.Duration.Count != parsedEnd.Entries.Count {
						summary.Valid = false
						summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("%s/%s: WAL histogram counts differ", window.Name, node.Name))
					}
					metrics = deltaMetrics(parsedStart, parsedEnd, window.Duration, window.AckedMessages)
					addMetricSummary(&total, metrics)
				}
				result.Nodes = append(result.Nodes, walNodeSummary{Name: node.Name, Metrics: metrics})
			}
			if total.SyncCount == 0 {
				summary.Valid = false
				summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("%s: total syncCount is zero", window.Name))
			}
			if total.SyncCount > 0 {
				if result.DurationSeconds > 0 {
					total.SyncsPerSecond = float64(total.SyncCount) / result.DurationSeconds
				}
				total.MeanEntriesPerSync = float64(total.EntriesSum) / float64(total.SyncCount)
				total.EntriesPerSync, total.SyncDurationSeconds = summedWALQuantiles(window.Nodes)
				if window.AckedMessages > 0 {
					v := float64(total.WALBytes) / float64(window.AckedMessages)
					total.WALBytesPerAckedMessage = &v
				}
			}
			result.Cluster = total
			summary.Windows = append(summary.Windows, result)
		}
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return WALSummary{}, fmt.Errorf("encode WAL summary: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(outputDir, "wal-summary.json"), data, 0o644); err != nil {
		return WALSummary{}, fmt.Errorf("write wal-summary.json: %w", err)
	}
	return summary, nil
}

func bytesPerMessage(bytes, acked uint64) *float64 {
	if acked == 0 {
		return nil
	}
	v := float64(bytes) / float64(acked)
	return &v
}

func parseWALScrape(data []byte) (walScrape, error) {
	result := walScrape{Duration: walHistogram{Buckets: map[float64]uint64{}}, Entries: walHistogram{Buckets: map[float64]uint64{}}}
	seen := map[string]bool{}
	countSeen := map[string]bool{}
	sumSeen := map[string]bool{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		name, labels := splitPromSample(fields[0])
		family := name
		for _, suffix := range []string{"_bucket", "_count", "_sum"} {
			if strings.HasSuffix(name, suffix) {
				family = strings.TrimSuffix(name, suffix)
				break
			}
		}
		if family != walDurationFamily && family != walEntriesFamily && family != walBytesFamily {
			continue
		}
		if len(fields) != 2 {
			return result, fmt.Errorf("invalid %s sample shape", name)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return result, fmt.Errorf("invalid %s sample", name)
		}
		if family == walBytesFamily {
			if seen[family] || name != family || value != math.Trunc(value) || value >= math.Exp2(64) {
				return result, fmt.Errorf("invalid or duplicate %s counter", family)
			}
			seen[family] = true
			result.Bytes = uint64(value)
			continue
		}
		seen[family] = true
		histogram := &result.Duration
		if family == walEntriesFamily {
			histogram = &result.Entries
		}
		switch name {
		case family + "_bucket":
			leText, ok := labels["le"]
			if !ok {
				return result, fmt.Errorf("%s bucket has no le label", family)
			}
			bound := math.Inf(1)
			if leText != "+Inf" {
				bound, err = strconv.ParseFloat(leText, 64)
				if err != nil || math.IsNaN(bound) || math.IsInf(bound, 0) || bound < 0 {
					return result, fmt.Errorf("invalid %s bucket bound", family)
				}
			}
			if value != math.Trunc(value) || value >= math.Exp2(64) {
				return result, fmt.Errorf("invalid %s bucket count", family)
			}
			if _, exists := histogram.Buckets[bound]; exists {
				return result, fmt.Errorf("duplicate %s bucket", family)
			}
			histogram.Buckets[bound] = uint64(value)
		case family + "_count":
			if countSeen[family] || value != math.Trunc(value) || value >= math.Exp2(64) {
				return result, fmt.Errorf("invalid or duplicate %s count", family)
			}
			histogram.Count = uint64(value)
			countSeen[family] = true
		case family + "_sum":
			if sumSeen[family] {
				return result, fmt.Errorf("duplicate %s sum", family)
			}
			if family == walEntriesFamily {
				if value != math.Trunc(value) || value >= math.Exp2(64) {
					return result, fmt.Errorf("invalid %s sum", family)
				}
				histogram.Sum = uint64(value)
			}
			sumSeen[family] = true
		default:
			return result, fmt.Errorf("unexpected sample %s in family %s", name, family)
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	for _, family := range []string{walDurationFamily, walEntriesFamily, walBytesFamily} {
		if !seen[family] {
			return result, fmt.Errorf("missing metric family %s", family)
		}
	}
	for _, pair := range []struct {
		name      string
		histogram walHistogram
	}{{walDurationFamily, result.Duration}, {walEntriesFamily, result.Entries}} {
		if _, ok := pair.histogram.Buckets[math.Inf(1)]; !ok {
			return result, fmt.Errorf("missing +Inf bucket for %s", pair.name)
		}
		if !countSeen[pair.name] {
			return result, fmt.Errorf("missing count for %s", pair.name)
		}
		if !sumSeen[pair.name] {
			return result, fmt.Errorf("missing sum for %s", pair.name)
		}
		if pair.histogram.Count != pair.histogram.Buckets[math.Inf(1)] {
			return result, fmt.Errorf("%s count differs from +Inf bucket", pair.name)
		}
	}
	return result, nil
}

func splitPromSample(sample string) (string, map[string]string) {
	labels := map[string]string{}
	if i := strings.IndexByte(sample, '{'); i >= 0 {
		name := sample[:i]
		body := strings.TrimSuffix(sample[i+1:], "}")
		for _, part := range strings.Split(body, ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				labels[kv[0]] = strings.Trim(kv[1], `"`)
			}
		}
		return name, labels
	}
	return sample, labels
}

func deltaMetrics(start, end walScrape, duration time.Duration, acked uint64) walMetricSummary {
	count := bucketDelta(start.Duration, end.Duration, math.Inf(1))
	entryCount := bucketDelta(start.Entries, end.Entries, math.Inf(1))
	entries := positiveDelta(start.Entries.Sum, end.Entries.Sum)
	walBytes := positiveDelta(start.Bytes, end.Bytes)
	metric := walMetricSummary{SyncCount: count, EntriesSum: entries, WALBytes: walBytes, WALBytesPerAckedMessage: bytesPerMessage(walBytes, acked)}
	if duration > 0 {
		metric.SyncsPerSecond = float64(count) / duration.Seconds()
	}
	if count > 0 {
		metric.MeanEntriesPerSync = float64(entries) / float64(count)
	}
	metric.EntriesPerSync = entryHistogramQuantiles(start.Entries, end.Entries, entryCount)
	metric.SyncDurationSeconds = durationHistogramQuantiles(start.Duration, end.Duration, count)
	return metric
}

func positiveDelta(start, end uint64) uint64 {
	if end < start {
		return 0
	}
	return end - start
}

func bucketDelta(start, end walHistogram, bound float64) uint64 {
	return positiveDelta(start.Buckets[bound], end.Buckets[bound])
}

func entryHistogramQuantiles(start, end walHistogram, count uint64) walEntryQuantiles {
	return walEntryQuantiles{
		P50: histogramQuantile(start, end, count, .50),
		P99: histogramQuantile(start, end, count, .99),
	}
}

func durationHistogramQuantiles(start, end walHistogram, count uint64) walDurationQuantiles {
	return walDurationQuantiles{
		P50:  histogramQuantile(start, end, count, .50),
		P99:  histogramQuantile(start, end, count, .99),
		P999: histogramQuantile(start, end, count, .999),
	}
}

func histogramQuantile(start, end walHistogram, count uint64, q float64) walQuantile {
	if count == 0 {
		return walQuantile{}
	}
	rank := uint64(math.Ceil(q * float64(count)))
	bounds := make([]float64, 0, len(end.Buckets))
	for bound := range end.Buckets {
		if _, ok := start.Buckets[bound]; ok {
			bounds = append(bounds, bound)
		}
	}
	sort.Float64s(bounds)
	for _, bound := range bounds {
		if positiveDelta(start.Buckets[bound], end.Buckets[bound]) >= rank {
			if math.IsInf(bound, 1) {
				return walQuantile{Overflow: true}
			}
			v := bound
			return walQuantile{UpperBound: &v}
		}
	}
	return walQuantile{Overflow: true}
}

func addMetricSummary(total *walMetricSummary, node walMetricSummary) {
	total.SyncCount += node.SyncCount
	total.EntriesSum += node.EntriesSum
	total.WALBytes += node.WALBytes
}

func summedWALQuantiles(nodes []walNodeInput) (walEntryQuantiles, walDurationQuantiles) {
	startDuration := walHistogram{Buckets: map[float64]uint64{}}
	endDuration := walHistogram{Buckets: map[float64]uint64{}}
	startEntries := walHistogram{Buckets: map[float64]uint64{}}
	endEntries := walHistogram{Buckets: map[float64]uint64{}}
	for _, node := range nodes {
		start, startErr := parseWALScrape(node.Start)
		end, endErr := parseWALScrape(node.End)
		if startErr != nil || endErr != nil {
			continue
		}
		for bound, count := range start.Duration.Buckets {
			startDuration.Buckets[bound] += count
		}
		for bound, count := range end.Duration.Buckets {
			endDuration.Buckets[bound] += count
		}
		for bound, count := range start.Entries.Buckets {
			startEntries.Buckets[bound] += count
		}
		for bound, count := range end.Entries.Buckets {
			endEntries.Buckets[bound] += count
		}
	}
	entryCount := bucketDelta(startEntries, endEntries, math.Inf(1))
	durationCount := bucketDelta(startDuration, endDuration, math.Inf(1))
	return entryHistogramQuantiles(startEntries, endEntries, entryCount),
		durationHistogramQuantiles(startDuration, endDuration, durationCount)
}
