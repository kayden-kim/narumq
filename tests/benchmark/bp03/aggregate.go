package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type messageRecord struct {
	Stream                     string `json:"stream"`
	QoS                        int    `json:"qos"`
	Sequence                   uint64 `json:"sequence"`
	Step                       int    `json:"step"`
	Phase                      string `json:"phase"`
	ScheduledAt                string `json:"scheduledAt"`
	SentAt                     string `json:"sentAt"`
	AckAt                      string `json:"ackAt"`
	CompleteAt                 string `json:"completeAt"`
	FirstReceivedAt            string `json:"firstReceivedAt"`
	ReceiveCount               int    `json:"receiveCount"`
	AckReasonCode              byte   `json:"ackReasonCode"`
	ScheduleLagNanoseconds     int64  `json:"scheduleLagNanoseconds"`
	AckLatencyNanoseconds      int64  `json:"ackLatencyNanoseconds"`
	CompleteLatencyNanoseconds int64  `json:"completeLatencyNanoseconds"`
	DeliveryLatencyNanoseconds int64  `json:"deliveryLatencyNanoseconds"`

	scheduled time.Time
	sent      time.Time
	acked     time.Time
	completed time.Time
	received  time.Time
	interval  int64
	sentOK    bool
	rejected  bool
	timedOut  bool
}

type messageKey struct {
	stream   string
	sequence uint64
}

type collector struct {
	mu                    sync.Mutex
	max                   uint64
	records               map[messageKey]*messageRecord
	order                 []*messageRecord
	protocolErrors        uint64
	sessionNotPresent     uint64
	overload              map[int]bool
	maxLag                map[int]int64
	offlineSessionPresent uint64
	reconnectMillis       []float64
	drainFinished         time.Time
}

func newCollector(maxMessages uint64) *collector {
	return &collector{
		max:      maxMessages,
		records:  make(map[messageKey]*messageRecord),
		overload: make(map[int]bool),
		maxLag:   make(map[int]int64),
	}
}

func (c *collector) recordOfflineSession(present bool) {
	c.mu.Lock()
	if present {
		c.offlineSessionPresent++
	} else {
		c.sessionNotPresent++
	}
	c.mu.Unlock()
}

func (c *collector) add(record messageRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if uint64(len(c.order)) >= c.max {
		return fmt.Errorf("message cap %d exceeded", c.max)
	}
	key := messageKey{stream: record.Stream, sequence: record.Sequence}
	if c.records[key] != nil {
		return fmt.Errorf("duplicate message record %s/%d", record.Stream, record.Sequence)
	}
	record.ScheduledAt = record.scheduled.UTC().Format(time.RFC3339Nano)
	c.records[key] = &record
	c.order = append(c.order, &record)
	return nil
}

func (c *collector) markSent(stream string, sequence uint64, sent time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil {
		return
	}
	record.sent = sent
	record.sentOK = true
	record.SentAt = sent.UTC().Format(time.RFC3339Nano)
	record.ScheduleLagNanoseconds = sent.Sub(record.scheduled).Nanoseconds()
	if record.Step != 0 && !strings.HasSuffix(record.Phase, "-measure") {
		return
	}
	if record.interval > 0 && record.ScheduleLagNanoseconds >= record.interval {
		c.overload[record.Step] = true
	}
	if record.ScheduleLagNanoseconds > c.maxLag[record.Step] {
		c.maxLag[record.Step] = record.ScheduleLagNanoseconds
	}
}

func (c *collector) acknowledge(stream string, sequence uint64, at time.Time, reason byte, complete bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || !record.sentOK || record.timedOut || record.rejected {
		return false
	}
	if !record.acked.IsZero() && !complete {
		return false
	}
	if record.acked.IsZero() {
		record.acked = at
		record.AckAt = at.UTC().Format(time.RFC3339Nano)
		record.AckLatencyNanoseconds = at.Sub(record.sent).Nanoseconds()
		record.AckReasonCode = reason
	}
	if reason >= 0x80 {
		record.AckReasonCode = reason
		record.rejected = true
		return true
	}
	if complete {
		record.completed = at
		record.CompleteAt = at.UTC().Format(time.RFC3339Nano)
		record.CompleteLatencyNanoseconds = at.Sub(record.sent).Nanoseconds()
	}
	return true
}

func (c *collector) timeout(stream string, sequence uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || !record.sentOK || record.rejected || record.timedOut || !record.completed.IsZero() {
		return false
	}
	if record.QoS == 1 && !record.acked.IsZero() {
		return false
	}
	record.timedOut = true
	return true
}

func (c *collector) receive(stream string, sequence uint64, at time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[messageKey{stream: stream, sequence: sequence}]
	if record == nil || !record.sentOK {
		return false
	}
	if record.Step > 0 && strings.HasSuffix(record.Phase, "-measure") && !c.drainFinished.IsZero() && at.After(c.drainFinished) {
		return false
	}
	record.ReceiveCount++
	if record.received.IsZero() {
		record.received = at
		record.FirstReceivedAt = at.UTC().Format(time.RFC3339Nano)
		record.DeliveryLatencyNanoseconds = at.Sub(record.sent).Nanoseconds()
	}
	return record.ReceiveCount > 1
}

func (c *collector) protocolError() {
	c.mu.Lock()
	c.protocolErrors++
	c.mu.Unlock()
}

type quantiles struct {
	P50  int64 `json:"p50"`
	P95  int64 `json:"p95"`
	P99  int64 `json:"p99"`
	P999 int64 `json:"p999"`
}

type qosStats struct {
	Offered                    int        `json:"offered"`
	Sent                       int        `json:"sent"`
	Acked                      int        `json:"acked"`
	AckRejected                int        `json:"ackRejected"`
	AckTimeouts                int        `json:"ackTimeouts"`
	Delivered                  int        `json:"delivered"`
	Gap                        int        `json:"gap"`
	Duplicate                  int        `json:"duplicate"`
	AckRatePerSecond           float64    `json:"ackRatePerSecond"`
	DeliveryRatePerSecond      float64    `json:"deliveryRatePerSecond"`
	AckLatencyNanoseconds      quantiles  `json:"ackLatencyNanoseconds"`
	DeliveryLatencyNanoseconds quantiles  `json:"deliveryLatencyNanoseconds"`
	CompleteLatencyNanoseconds *quantiles `json:"completeLatencyNanoseconds,omitempty"`
	completeCount              int        `json:"-"`
}
type offlineQoSStats struct {
	Offered   int `json:"offered"`
	Sent      int `json:"sent"`
	Acked     int `json:"acked"`
	Delivered int `json:"delivered"`
	Gap       int `json:"gap"`
	Duplicate int `json:"duplicate"`
}

type stepSummary struct {
	Step                      int                 `json:"step"`
	PerStreamRatePerSecond    uint64              `json:"perStreamRatePerSecond"`
	AggregateRatePerSecond    uint64              `json:"aggregateRatePerSecond"`
	MeasureStartedAt          string              `json:"measureStartedAt"`
	MeasureFinishedAt         string              `json:"measureFinishedAt"`
	MaxScheduleLagNanoseconds int64               `json:"maxScheduleLagNanoseconds"`
	GeneratorOverload         bool                `json:"generatorOverload"`
	QoS                       map[string]qosStats `json:"qos"`
}

type offlineSummary struct {
	QoS                  map[string]offlineQoSStats `json:"qos"`
	SessionPresent       int                        `json:"sessionPresent"`
	Sessions             int                        `json:"sessions"`
	ReconnectDrainMillis struct {
		P50 float64 `json:"p50"`
		Max float64 `json:"max"`
	} `json:"reconnectDrainMillis"`
}

type workloadSummary struct {
	SchemaVersion     int            `json:"schemaVersion"`
	Target            string         `json:"target"`
	ProfileID         string         `json:"profileId"`
	Purpose           string         `json:"purpose"`
	ReleaseGating     bool           `json:"releaseGating"`
	Steps             []stepSummary  `json:"steps"`
	Offline           offlineSummary `json:"offline"`
	ProtocolErrors    uint64         `json:"protocolErrors"`
	SessionNotPresent uint64         `json:"sessionNotPresent"`
	Valid             bool           `json:"valid"`
	FailureReasons    []string       `json:"failureReasons"`
}

func nearestRank(values []int64, percentile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	rank := int(math.Ceil(percentile * float64(len(values))))
	if rank < 1 {
		rank = 1
	}
	return values[rank-1]
}

func (c *collector) summarize(profile Profile, target string, windows map[int][2]time.Time) workloadSummary {
	c.mu.Lock()
	defer c.mu.Unlock()

	summary := workloadSummary{
		SchemaVersion: 1,
		Target:        target,
		ProfileID:     profile.ProfileID,
		Purpose:       profile.Purpose,
		Valid:         true,
		Steps:         make([]stepSummary, 0, len(profile.RatesPerStreamPerSecond)),
	}
	for index, rate := range profile.RatesPerStreamPerSecond {
		step := index + 1
		entry := stepSummary{
			Step:                      step,
			PerStreamRatePerSecond:    rate,
			AggregateRatePerSecond:    rate * uint64(profile.PairsPerQoS*2),
			MaxScheduleLagNanoseconds: c.maxLag[step],
			GeneratorOverload:         c.overload[step],
			QoS:                       make(map[string]qosStats, 2),
		}
		if bounds := windows[step]; !bounds[0].IsZero() {
			entry.MeasureStartedAt = bounds[0].UTC().Format(time.RFC3339Nano)
			entry.MeasureFinishedAt = bounds[1].UTC().Format(time.RFC3339Nano)
		}
		offered := int(rate*uint64(profile.MeasureMillis)/1000) * profile.PairsPerQoS
		for _, qos := range []int{1, 2} {
			stats := c.stats(qos, step, false, offered, float64(profile.MeasureMillis)/1000)
			entry.QoS[fmt.Sprint(qos)] = stats
			if invalidQos(stats, qos) {
				summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("step %02d QoS %d message invariant failed", step, qos))
			}
		}
		if entry.GeneratorOverload {
			summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("step %02d generator overload", step))
		}
		summary.Steps = append(summary.Steps, entry)
	}

	summary.Offline.QoS = make(map[string]offlineQoSStats, 2)
	summary.Offline.Sessions = profile.PairsPerQoS * 2
	summary.Offline.SessionPresent = int(c.offlineSessionPresent)
	offlineOffered := profile.OfflineMessagesPerStream * profile.PairsPerQoS
	for _, qos := range []int{1, 2} {
		stats := c.stats(qos, 0, true, offlineOffered, 1)
		summary.Offline.QoS[fmt.Sprint(qos)] = offlineQoSStats{
			Offered:   stats.Offered,
			Sent:      stats.Sent,
			Acked:     stats.Acked,
			Delivered: stats.Delivered,
			Gap:       stats.Gap,
			Duplicate: stats.Duplicate,
		}
		if stats.Offered != stats.Sent || stats.Acked != stats.Sent || stats.AckRejected != 0 || stats.AckTimeouts != 0 || stats.Gap != 0 || (qos == 2 && (stats.Duplicate != 0 || stats.completeCount != stats.Sent)) {
			summary.FailureReasons = append(summary.FailureReasons, fmt.Sprintf("offline QoS %d delivery or acknowledgement invariant failed", qos))
		}
	}
	if c.overload[0] {
		summary.FailureReasons = append(summary.FailureReasons, "offline generator overload")
	}
	if summary.Offline.SessionPresent != summary.Offline.Sessions || c.sessionNotPresent != 0 {
		summary.FailureReasons = append(summary.FailureReasons, "one or more persistent sessions were not present after reconnect")
	}
	if len(c.reconnectMillis) > 0 {
		durations := append([]float64(nil), c.reconnectMillis...)
		sort.Float64s(durations)
		summary.Offline.ReconnectDrainMillis.Max = durations[len(durations)-1]
		summary.Offline.ReconnectDrainMillis.P50 = durations[(len(durations)-1)/2]
	}
	summary.ProtocolErrors = c.protocolErrors
	summary.SessionNotPresent = c.sessionNotPresent
	if c.protocolErrors > 0 {
		summary.FailureReasons = append(summary.FailureReasons, "MQTT protocol errors were observed")
	}
	summary.Valid = len(summary.FailureReasons) == 0
	return summary
}

func invalidQos(stats qosStats, qos int) bool {
	return stats.Offered != stats.Sent ||
		stats.Acked != stats.Sent ||
		stats.AckRejected != 0 ||
		stats.AckTimeouts != 0 ||
		stats.Gap != 0 ||
		(qos == 2 && (stats.Duplicate != 0 || stats.completeCount != stats.Sent))
}

func (c *collector) stats(qos, step int, offline bool, offered int, seconds float64) qosStats {
	var stats qosStats
	var ackLatencies []int64
	var deliveryLatencies []int64
	var completeLatencies []int64
	stats.Offered = offered
	for _, record := range c.order {
		if record.QoS != qos || ((record.Step == 0) != offline) {
			continue
		}
		if !offline && (record.Step != step || !strings.HasSuffix(record.Phase, "-measure")) {
			continue
		}
		if !record.sentOK {
			continue
		}
		stats.Sent++
		if record.rejected {
			stats.AckRejected++
		}
		if record.timedOut {
			stats.AckTimeouts++
		}
		if !record.acked.IsZero() {
			stats.Acked++
			ackLatencies = append(ackLatencies, record.AckLatencyNanoseconds)
		}
		if record.received.IsZero() {
			if !record.acked.IsZero() {
				stats.Gap++
			}
		} else {
			stats.Delivered++
			deliveryLatencies = append(deliveryLatencies, record.DeliveryLatencyNanoseconds)
			if record.ReceiveCount > 1 {
				stats.Duplicate += record.ReceiveCount - 1
			}
		}
		if !record.completed.IsZero() {
			completeLatencies = append(completeLatencies, record.CompleteLatencyNanoseconds)
			stats.completeCount++
		}
	}
	stats.AckLatencyNanoseconds = quantileSet(ackLatencies)
	stats.DeliveryLatencyNanoseconds = quantileSet(deliveryLatencies)
	if qos == 2 {
		quantiles := quantileSet(completeLatencies)
		stats.CompleteLatencyNanoseconds = &quantiles
	}
	if seconds > 0 {
		stats.AckRatePerSecond = float64(stats.Acked) / seconds
		stats.DeliveryRatePerSecond = float64(stats.Delivered) / seconds
	}
	return stats
}

func quantileSet(values []int64) quantiles {
	return quantiles{
		P50:  nearestRank(append([]int64(nil), values...), 0.5),
		P95:  nearestRank(append([]int64(nil), values...), 0.95),
		P99:  nearestRank(append([]int64(nil), values...), 0.99),
		P999: nearestRank(append([]int64(nil), values...), 0.999),
	}
}

func (c *collector) messageSnapshot() []messageRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]messageRecord, len(c.order))
	for index, record := range c.order {
		records[index] = *record
	}
	return records
}

func (c *collector) writeMessages(path string) error {
	records := c.messageSnapshot()
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return errors.Join(err, file.Close())
		}
	}
	return file.Close()
}

func writeWorkloadSummary(dir string, summary workloadSummary) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "workload-summary.json"), append(data, '\n'), 0o644)
}

func (c *collector) ackedForStep(step int) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var count uint64
	for _, record := range c.order {
		if record.Step == step && (step == 0 || strings.HasSuffix(record.Phase, "-measure")) && !record.acked.IsZero() {
			count++
		}
	}
	return count
}

func (c *collector) measureDelivered() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range c.order {
		if record.Step == 0 || !strings.HasSuffix(record.Phase, "-measure") || !record.sentOK {
			continue
		}
		if record.timedOut || record.rejected {
			continue
		}
		if record.acked.IsZero() || record.received.IsZero() {
			return false
		}
	}
	return true
}
func (c *collector) markDrainFinished(at time.Time) {
	c.mu.Lock()
	c.drainFinished = at
	c.mu.Unlock()
}

func (c *collector) offlineExchangesComplete(streams []streamSpec, perStream int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stream := range streams {
		count := 0
		for _, record := range c.order {
			if record.Stream != stream.name || record.Step != 0 {
				continue
			}
			count++
			if !record.sentOK || record.acked.IsZero() || record.received.IsZero() ||
				record.rejected || record.timedOut ||
				(record.QoS == 2 && record.completed.IsZero()) {
				return false
			}
		}
		if count != perStream {
			return false
		}
	}
	return true
}

func (c *collector) lastOfflineReceipt(stream string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var latest time.Time
	for _, record := range c.order {
		if record.Stream == stream && record.Step == 0 && record.received.After(latest) {
			latest = record.received
		}
	}
	return latest, !latest.IsZero()
}
