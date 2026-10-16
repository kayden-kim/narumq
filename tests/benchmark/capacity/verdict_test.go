package main

import (
	"strings"
	"testing"
	"time"
)

func passingInput() verdictInput {
	evidence := measureEvidence{GeneratorCPUKnown: true, GeneratorCPUPercent: 40}
	for qos := range 3 {
		evidence.Published[qos] = 100
		evidence.Latency[qos] = qosLatency{P99: 20 * time.Millisecond, Count: 100}
	}
	return verdictInput{
		P99Limit: 100 * time.Millisecond, RecoveryLimit: 30 * time.Second, CPUSaturationLimit: 80,
		Measure: [2]measureEvidence{evidence, evidence},
		RTO:     5 * time.Second, Recovered: true, RecoveryEvaluated: true,
	}
}

func joined(r verdictResult) string { return strings.Join(r.Reasons, "; ") }

func TestVerdictP99ExactlyAtLimitPassesAndAboveFails(t *testing.T) {
	in := passingInput()
	in.Measure[1].Latency[1].P99 = 100 * time.Millisecond
	if got := computeVerdict(in); got.Verdict != verdictPass {
		t.Fatalf("p99 == limit: %s (%s)", got.Verdict, joined(got))
	}
	in.Measure[1].Latency[1].P99 = 100*time.Millisecond + time.Microsecond
	got := computeVerdict(in)
	if got.Verdict != verdictFail || !strings.Contains(joined(got), "measureN1 QoS1 p99") {
		t.Fatalf("p99 above limit: %s (%s)", got.Verdict, joined(got))
	}
}

func TestVerdictGapFailsButQoS1DuplicateAndQoS0LossOnlyRecord(t *testing.T) {
	in := passingInput()
	in.AckedGaps[1] = 1
	if got := computeVerdict(in); got.Verdict != verdictFail || !strings.Contains(joined(got), "QoS1 ACKed gap") {
		t.Fatalf("QoS1 gap: %s (%s)", got.Verdict, joined(got))
	}
	in = passingInput()
	in.QoS2Duplicates = 1
	if got := computeVerdict(in); got.Verdict != verdictFail || !strings.Contains(joined(got), "QoS2 duplicate") {
		t.Fatalf("QoS2 duplicate: %s (%s)", got.Verdict, joined(got))
	}
	// QoS 1 duplicates and QoS 0 loss are not part of verdictInput: they are recorded only.
	if got := computeVerdict(passingInput()); got.Verdict != verdictPass {
		t.Fatalf("baseline: %s (%s)", got.Verdict, joined(got))
	}
}

func TestVerdictInvalidTakesPrecedenceOverFail(t *testing.T) {
	in := passingInput()
	in.AckedGaps[2] = 3
	in.Measure[0].ScheduleOverloads = 2
	got := computeVerdict(in)
	if got.Verdict != verdictInvalid {
		t.Fatalf("verdict %s, want invalid", got.Verdict)
	}
	if strings.Contains(joined(got), "gap") || len(got.FailConditions) == 0 {
		t.Fatalf("invalid reasons must exclude fail causes but keep them as evidence: reasons %q fail %q", got.Reasons, got.FailConditions)
	}
}

func TestVerdictInvalidCauses(t *testing.T) {
	cases := map[string]func(*verdictInput){
		"tenant rejection": func(in *verdictInput) { in.TenantRejections = 1 },
		"generator cpu":    func(in *verdictInput) { in.Measure[1].GeneratorCPUPercent = 80.1 },
		"controller":       func(in *verdictInput) { in.InvalidReasons = []string{"controller reported failure"} },
	}
	for name, mutate := range cases {
		in := passingInput()
		mutate(&in)
		if got := computeVerdict(in); got.Verdict != verdictInvalid {
			t.Errorf("%s: verdict %s (%s)", name, got.Verdict, joined(got))
		}
	}
	in := passingInput()
	in.Measure[0].GeneratorCPUPercent = 80
	if got := computeVerdict(in); got.Verdict != verdictPass {
		t.Errorf("cpu exactly at the saturation limit must not be saturated: %s", got.Verdict)
	}
	in = passingInput()
	in.Measure[0].GeneratorCPUKnown = false
	in.Measure[0].GeneratorCPUPercent = 99
	if got := computeVerdict(in); got.Verdict != verdictPass {
		t.Errorf("unknown cpu must not invalidate: %s", got.Verdict)
	}
}

func TestVerdictRecoveryRules(t *testing.T) {
	in := passingInput()
	in.RTO = 30 * time.Second
	if got := computeVerdict(in); got.Verdict != verdictPass {
		t.Fatalf("RTO == limit: %s (%s)", got.Verdict, joined(got))
	}
	in.RTO = 30*time.Second + time.Nanosecond
	if got := computeVerdict(in); got.Verdict != verdictFail {
		t.Fatalf("RTO above limit: %s", got.Verdict)
	}
	in = passingInput()
	in.Recovered = false
	if got := computeVerdict(in); got.Verdict != verdictFail || !strings.Contains(joined(got), "not recovered") {
		t.Fatalf("unrecovered: %s (%s)", got.Verdict, joined(got))
	}
	in.RecoveryEvaluated = false
	if got := computeVerdict(in); got.Verdict != verdictPass {
		t.Fatalf("recovery not evaluated must not fail by itself: %s", got.Verdict)
	}
	in = passingInput()
	in.SessionPresentFails = 1
	if got := computeVerdict(in); got.Verdict != verdictFail {
		t.Fatalf("SessionPresent=false: %s", got.Verdict)
	}
}

func TestVerdictMeasureWindowDefects(t *testing.T) {
	cases := map[string]func(*measureEvidence){
		"ack timeout":        func(e *measureEvidence) { e.AckTimeouts = 1 },
		"protocol error":     func(e *measureEvidence) { e.ProtocolErrors = 1 },
		"disconnect":         func(e *measureEvidence) { e.UnplannedDisconnects = 1 },
		"credit starved":     func(e *measureEvidence) { e.CreditStarvations = 1 },
		"no latency samples": func(e *measureEvidence) { e.Latency[0].Count = 0 },
	}
	for name, mutate := range cases {
		in := passingInput()
		mutate(&in.Measure[0])
		if got := computeVerdict(in); got.Verdict != verdictFail {
			t.Errorf("%s: verdict %s (%s)", name, got.Verdict, joined(got))
		}
	}
}
