package main

import (
	"path/filepath"
	"time"
)

const latencyAttribution = "A latency sample is attributed to the window in which the first copy was received; publish time is taken just before the socket write."

func latencyOf(h *histogram) qosLatency {
	return qosLatency{P50: h.Quantile(0.50), P99: h.Quantile(0.99), P999: h.Quantile(0.999), Max: h.Max(), Count: h.Count()}
}

func (st *stepRun) evidence(index int, w windowID) measureEvidence {
	stats := st.col.windowStats(w)
	evidence := measureEvidence{
		Published: stats.Published, AckTimeouts: stats.AckTimeouts, ProtocolErrors: stats.ProtocolErrors,
		UnplannedDisconnects: stats.UnplannedDisconnects, ScheduleOverloads: stats.ScheduleOverloads, CreditStarvations: stats.CreditStarvations,
	}
	for qos := range 3 {
		evidence.Latency[qos] = latencyOf(st.col.hist[index][qos])
	}
	evidence.GeneratorCPUPercent, evidence.GeneratorCPUKnown = st.averageCPU(w)
	return evidence
}

// summarize computes the verdict and the step-summary document.
func (st *stepRun) summarize(started time.Time, gaps gapResult, outstanding int64) stepSummary {
	p := st.r.profile
	col := st.col
	input := verdictInput{
		P99Limit:                     time.Duration(p.LatencyP99LimitMillis) * time.Millisecond,
		RecoveryLimit:                time.Duration(p.RecoveryTimeoutMillis) * time.Millisecond,
		CPUSaturationLimit:           p.GeneratorCPUSaturationPercent,
		InvalidReasons:               append([]string(nil), st.invalid...),
		TenantLimitRejections:        col.quota.total(quotaTenantLimit),
		ServerBackpressureRejections: col.quota.total(quotaServerBackpressure),
		UnclassifiedRejections:       col.quota.total(quotaUnclassified),
		SetupFailure:                 st.setupErr,
		Measure:                      [2]measureEvidence{st.evidence(0, winMeasureN), st.evidence(1, winMeasureN1)},
		AckedGaps:                    gaps.Gaps,
		CleanSessionLossOverLimit:    lossOverLimit(gaps),
		CleanSessionLossMaxMillis:    lossMaxMillis(gaps),
		QoS2Duplicates:               col.duplicates[2].Load(),
		Unexpected:                   col.unexpected.Load(),
		RTO:                          st.recovery.rto, Recovered: st.recovery.recovered, RecoveryEvaluated: st.recovery.evaluated,
		SessionPresentFails: col.sessionPresentFailures.Load(),
	}
	result := computeVerdict(input)

	summary := stepSummary{
		ActiveClusterFeatures: append([]string{}, st.r.features...),
		SchemaVersion:         1, Step: st.cfg.Step, Run: st.cfg.Run, Cells: st.cfg.Cells, RatePerStreamPerSecond: st.cfg.Rate,
		OfferedPublishPerSecond: st.cfg.Offered(), Connections: st.cfg.Cells * cellClients,
		StartedAt: formatTime(started), FinishedAt: formatTime(time.Now()),
		Verdict: result.Verdict, Reasons: result.Reasons, FailConditions: result.FailConditions,
		ReleaseGating: false, LatencyAttribution: latencyAttribution,
		P99LimitMillis: p.LatencyP99LimitMillis, RecoveryLimitMillis: p.RecoveryTimeoutMillis,
		Windows: map[string]windowSpan{}, Counters: map[string]windowStats{},
		Latency: map[string][3]qosLatency{"measureN": input.Measure[0].Latency, "measureN1": input.Measure[1].Latency},
		Markers: map[string]string{},
	}
	tls := countTLSClients(p) * st.cfg.Cells
	summary.TLSClients, summary.TCPClients = tls, summary.Connections-tls
	for w := windowID(0); w < numWindows; w++ {
		summary.Counters[w.String()] = col.windowStats(w)
		st.mu.Lock()
		span := st.spans[w]
		st.mu.Unlock()
		if !span.start.IsZero() {
			summary.Windows[w.String()] = windowSpan{StartedAt: formatTime(span.start), Millis: st.spanMillis(w)}
		}
	}
	if seconds := st.spanMillis(winMeasureN1) / 1000; seconds > 0 {
		delivered := col.windowStats(winMeasureN1).Delivered
		for qos := range 3 {
			summary.DeliveryRatesN1[qos] = float64(delivered[qos]) / seconds
		}
	}
	summary.Delivery = deliverySummary{
		gapResult:             gaps,
		QoS1Duplicates:        col.duplicates[1].Load(),
		QoS2Duplicates:        col.duplicates[2].Load(),
		UnexpectedDeliveries:  col.unexpected.Load(),
		StaleTokenDeliveries:  col.stale.Load(),
		RetainedReplays:       col.retainedReplays.Load(),
		SetupFailures:         col.setupFailures.Load(),
		OutstandingAfterDrain: outstanding,
	}
	summary.QuotaRejections = col.quota.summary()

	recovery := recoverySummary{
		LeaderNodeID: st.leader, KillMarkerStates: append([]string{}, st.markerStates...),
		Recovered: st.recovery.recovered, RTOMillis: float64(st.recovery.rto) / float64(time.Millisecond),
		AffectedClients: col.affected.Load(), SessionPresentFailures: col.sessionPresentFailures.Load(),
	}
	if t0 := col.t0.Load(); t0 >= 0 {
		recovery.KillingObservedMillis = float64(t0) / float64(time.Millisecond)
	}
	if injection := col.injection.Load(); injection >= 0 {
		recovery.InjectionMillis = float64(injection) / float64(time.Millisecond)
	}
	if st.kill.lastMarker.State != "" {
		target := st.kill.lastMarker.Target
		recovery.KillTarget = &target
	}
	summary.Recovery = recovery
	st.mu.Lock()
	peak := st.peakRSS
	st.mu.Unlock()
	cpuN, knownN := st.averageCPU(winMeasureN)
	cpuN1, knownN1 := st.averageCPU(winMeasureN1)
	summary.Generator = generatorSummary{
		CPUKnown: knownN && knownN1, CPUPercentN: cpuN, CPUPercentN1: cpuN1,
		CPULimitPercent: p.GeneratorCPUSaturationPercent, CPUDenominator: p.GeneratorCPUs, PeakRSSBytes: peak,
	}
	for _, name := range []string{"ready", "kill", "kill-ack", "n1-done"} {
		path := markerPath(st.r.controlDir, st.cfg.Step, st.cfg.Run, name)
		if fileExists(path) {
			summary.Markers[name] = "control/" + filepath.Base(path)
		}
	}
	return summary
}
