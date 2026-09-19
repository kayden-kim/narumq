package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Percentiles contains nearest-rank latency values in nanoseconds.
type Percentiles struct {
	P50Nanoseconds  int64 `json:"p50Nanoseconds"`
	P95Nanoseconds  int64 `json:"p95Nanoseconds"`
	P99Nanoseconds  int64 `json:"p99Nanoseconds"`
	P999Nanoseconds int64 `json:"p999Nanoseconds"`
}

// WorkloadSummary is the stable aggregate contract for one BP-05 target.
type WorkloadSummary struct {
	SchemaVersion                  int         `json:"schemaVersion"`
	Target                         string      `json:"target"`
	ProfileID                      string      `json:"profileId"`
	Purpose                        string      `json:"purpose"`
	RequestedInitialConnections    int         `json:"requestedInitialConnections"`
	AttemptedInitialConnections    int         `json:"attemptedInitialConnections"`
	SucceededInitialConnections    int         `json:"succeededInitialConnections"`
	FailedInitialConnections       int         `json:"failedInitialConnections"`
	InitialConnectionRatePerSecond float64     `json:"initialConnectionRatePerSecond"`
	InitialConnectLatency          Percentiles `json:"initialConnectLatencyNanoseconds"`
	MaxScheduleLagNanoseconds      int64       `json:"maxScheduleLagNanoseconds"`
	IdleSurvivors                  int         `json:"idleSurvivors"`
	PingSent                       uint64      `json:"pingSent"`
	PingReceived                   uint64      `json:"pingReceived"`
	PingErrors                     uint64      `json:"pingErrors"`
	ReconnectRequested             int         `json:"reconnectRequested"`
	ReconnectAttempted             int         `json:"reconnectAttempted"`
	ReconnectSucceeded             int         `json:"reconnectSucceeded"`
	ReconnectFailed                int         `json:"reconnectFailed"`
	ReconnectRatePerSecond         float64     `json:"reconnectRatePerSecond"`
	ReconnectLatency               Percentiles `json:"reconnectLatencyNanoseconds"`
	FinalPingRequested             int         `json:"finalPingRequested"`
	FinalPingSucceeded             int         `json:"finalPingSucceeded"`
	FinalPingErrors                uint64      `json:"finalPingErrors"`
	ProtocolErrors                 uint64      `json:"protocolErrors"`
	TransportErrors                uint64      `json:"transportErrors"`
	GeneratorOverload              uint64      `json:"generatorOverload"`
	ErrorCount                     uint64      `json:"errorCount"`
	Valid                          bool        `json:"valid"`
	ReleaseGating                  bool        `json:"releaseGating"`
}

func buildWorkloadSummary(profile Profile, target string, counters workloadCounters) WorkloadSummary {
	initialRate := ratePerSecond(counters.initialSucceeded, counters.initialDuration)
	reconnectRate := ratePerSecond(counters.reconnectSucceeded, counters.reconnectDuration)
	initialPercentiles := nearestRankPercentiles(counters.initialLatencies)
	reconnectPercentiles := nearestRankPercentiles(counters.reconnectLatencies)
	maxScheduleLag := counters.maxScheduleLag.Nanoseconds()
	valid := counters.initialAttempted == profile.ConnectionCount && counters.initialSucceeded == profile.ConnectionCount && counters.initialFailed == 0 && counters.idleSurvivors == profile.ConnectionCount && counters.pingErrors == 0 && counters.reconnectAttempted == profile.ReconnectCount && counters.reconnectSucceeded == profile.ReconnectCount && counters.reconnectFailed == 0 && counters.finalPingRequested == counters.finalPingSucceeded && counters.finalPingErrors == 0 && counters.protocolErrors == 0 && counters.transportErrors == 0 && counters.generatorOverload == 0
	errorCount := uint64(counters.initialFailed+counters.reconnectFailed) + counters.pingErrors + counters.finalPingErrors + counters.protocolErrors + counters.transportErrors + counters.generatorOverload
	return WorkloadSummary{
		SchemaVersion:                  1,
		Target:                         target,
		ProfileID:                      profile.ProfileID,
		Purpose:                        profile.Purpose,
		RequestedInitialConnections:    profile.ConnectionCount,
		AttemptedInitialConnections:    counters.initialAttempted,
		SucceededInitialConnections:    counters.initialSucceeded,
		FailedInitialConnections:       counters.initialFailed,
		InitialConnectionRatePerSecond: initialRate,
		InitialConnectLatency:          initialPercentiles,
		MaxScheduleLagNanoseconds:      maxScheduleLag,
		IdleSurvivors:                  counters.idleSurvivors,
		PingSent:                       counters.pingSent,
		PingReceived:                   counters.pingReceived,
		PingErrors:                     counters.pingErrors,
		ReconnectRequested:             profile.ReconnectCount,
		ReconnectAttempted:             counters.reconnectAttempted,
		ReconnectSucceeded:             counters.reconnectSucceeded,
		ReconnectFailed:                counters.reconnectFailed,
		ReconnectRatePerSecond:         reconnectRate,
		ReconnectLatency:               reconnectPercentiles,
		FinalPingRequested:             counters.finalPingRequested,
		FinalPingSucceeded:             counters.finalPingSucceeded,
		FinalPingErrors:                counters.finalPingErrors,
		ProtocolErrors:                 counters.protocolErrors,
		TransportErrors:                counters.transportErrors,
		GeneratorOverload:              counters.generatorOverload,
		ErrorCount:                     errorCount,
		Valid:                          valid,
		ReleaseGating:                  false,
	}
}

func ratePerSecond(count int, duration time.Duration) float64 {
	if count <= 0 || duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
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

func WriteWorkloadSummary(outputDir string, summary WorkloadSummary) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("encode workload summary: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(outputDir, "workload-summary.json"), data, 0o644); err != nil {
		return fmt.Errorf("write workload-summary.json: %w", err)
	}
	return nil
}
