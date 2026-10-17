package main

import (
	"fmt"
	"time"
)

const (
	verdictPass    = "pass"
	verdictFail    = "fail"
	verdictInvalid = "invalid"
)

// qosLatency is one per-QoS latency window result.
type qosLatency struct {
	P50   time.Duration `json:"p50Nanos"`
	P99   time.Duration `json:"p99Nanos"`
	P999  time.Duration `json:"p999Nanos"`
	Max   time.Duration `json:"maxNanos"`
	Count uint64        `json:"samples"`
}

// measureEvidence is the gating evidence of one measure window.
type measureEvidence struct {
	Latency              [3]qosLatency
	Published            [3]uint64
	AckTimeouts          uint64
	ProtocolErrors       uint64
	UnplannedDisconnects uint64
	ScheduleOverloads    uint64
	CreditStarvations    uint64
	GeneratorCPUPercent  float64
	GeneratorCPUKnown    bool
}

// verdictInput is every observation the §3.9 판정 needs for one step-run.
type verdictInput struct {
	P99Limit           time.Duration
	RecoveryLimit      time.Duration
	CPUSaturationLimit float64
	InvalidReasons     []string // controller/marker/readiness/resync/setup-quota/cleanup failures
	// Quota-class rejections by class (§3.9 "Quota 계열 거절 판정"): tenant limits make the step
	// invalid; server backpressure and unclassified rejections are fail conditions.
	TenantLimitRejections        uint64
	ServerBackpressureRejections uint64
	UnclassifiedRejections       uint64
	SetupFailure                 string
	Measure                      [2]measureEvidence // measureN, measureN1
	AckedGaps                    [3]uint64
	// CleanSessionLossOverLimit counts clean-session losses older than P99Limit (disconnect minus
	// publish) and CleanSessionLossMaxMillis is the largest loss age; the former is a fail.
	CleanSessionLossOverLimit uint64
	CleanSessionLossMaxMillis float64
	QoS2Duplicates            uint64
	Unexpected                uint64
	RTO                       time.Duration
	Recovered                 bool
	RecoveryEvaluated         bool // false when the step ended before the failure phase ran
	SessionPresentFails       uint64
}

type verdictResult struct {
	Verdict        string
	Reasons        []string
	FailConditions []string
}

var measureNames = [2]string{"measureN", "measureN1"}

// computeVerdict applies the §3.9 rules. Invalid takes precedence over fail: an invalid step
// says nothing about broker capacity, so its reasons are the invalid causes only; fail
// conditions seen in the same step are kept as evidence in FailConditions.
func computeVerdict(in verdictInput) verdictResult {
	invalid := append([]string(nil), in.InvalidReasons...)
	if in.TenantLimitRejections > 0 {
		invalid = append(invalid, fmt.Sprintf("tenant limit rejection observed %d times", in.TenantLimitRejections))
	}
	for w, evidence := range in.Measure {
		if evidence.ScheduleOverloads > 0 {
			invalid = append(invalid, fmt.Sprintf("generator saturated in %s: %d publish schedule lag >= interval", measureNames[w], evidence.ScheduleOverloads))
		}
		if evidence.GeneratorCPUKnown && evidence.GeneratorCPUPercent > in.CPUSaturationLimit {
			invalid = append(invalid, fmt.Sprintf("generator saturated in %s: CPU %.1f%% > %.1f%%", measureNames[w], evidence.GeneratorCPUPercent, in.CPUSaturationLimit))
		}
	}

	var fail []string
	if in.SetupFailure != "" {
		fail = append(fail, "setup incomplete: "+in.SetupFailure)
	}
	if in.ServerBackpressureRejections > 0 {
		fail = append(fail, fmt.Sprintf("server backpressure rejection observed %d times", in.ServerBackpressureRejections))
	}
	if in.UnclassifiedRejections > 0 {
		fail = append(fail, fmt.Sprintf("unclassified quota rejection observed %d times (reason code 0x97 or 0x9F without a known class)", in.UnclassifiedRejections))
	}
	for w, evidence := range in.Measure {
		for qos := range 3 {
			latency := evidence.Latency[qos]
			switch {
			case latency.Count == 0 && evidence.Published[qos] > 0:
				fail = append(fail, fmt.Sprintf("%s QoS%d: %d published but no delivery latency samples", measureNames[w], qos, evidence.Published[qos]))
			case latency.Count > 0 && latency.P99 > in.P99Limit:
				fail = append(fail, fmt.Sprintf("%s QoS%d p99 %s exceeds %s", measureNames[w], qos, latency.P99, in.P99Limit))
			}
		}
		if evidence.AckTimeouts > 0 {
			fail = append(fail, fmt.Sprintf("%s: %d ACK timeouts", measureNames[w], evidence.AckTimeouts))
		}
		if evidence.ProtocolErrors > 0 {
			fail = append(fail, fmt.Sprintf("%s: %d protocol errors", measureNames[w], evidence.ProtocolErrors))
		}
		if evidence.UnplannedDisconnects > 0 {
			fail = append(fail, fmt.Sprintf("%s: %d unplanned disconnects", measureNames[w], evidence.UnplannedDisconnects))
		}
		if evidence.CreditStarvations > 0 {
			fail = append(fail, fmt.Sprintf("%s: publish inflight credit starved %d times (broker ACKs slower than the inflight window)", measureNames[w], evidence.CreditStarvations))
		}
	}
	for qos := 1; qos <= 2; qos++ {
		if in.AckedGaps[qos] > 0 {
			fail = append(fail, fmt.Sprintf("QoS%d ACKed gap %d", qos, in.AckedGaps[qos]))
		}
	}
	if in.CleanSessionLossOverLimit > 0 {
		fail = append(fail, fmt.Sprintf("clean-session loss older than latency limit: %d losses, max age %.1fms > %s",
			in.CleanSessionLossOverLimit, in.CleanSessionLossMaxMillis, in.P99Limit))
	}
	if in.QoS2Duplicates > 0 {
		fail = append(fail, fmt.Sprintf("QoS2 duplicate %d", in.QoS2Duplicates))
	}
	if in.Unexpected > 0 {
		fail = append(fail, fmt.Sprintf("unexpected deliveries %d", in.Unexpected))
	}
	switch {
	case !in.RecoveryEvaluated:
	case !in.Recovered:
		fail = append(fail, fmt.Sprintf("not recovered within %s (observed %s)", in.RecoveryLimit, in.RTO))
	case in.RTO > in.RecoveryLimit:
		fail = append(fail, fmt.Sprintf("RTO %s exceeds %s", in.RTO, in.RecoveryLimit))
	}
	if in.SessionPresentFails > 0 {
		fail = append(fail, fmt.Sprintf("persistent session lost on reconnect %d times (SessionPresent=false)", in.SessionPresentFails))
	}

	switch {
	case len(invalid) > 0:
		return verdictResult{Verdict: verdictInvalid, Reasons: invalid, FailConditions: fail}
	case len(fail) > 0:
		return verdictResult{Verdict: verdictFail, Reasons: fail, FailConditions: fail}
	default:
		return verdictResult{Verdict: verdictPass, Reasons: []string{}, FailConditions: []string{}}
	}
}
