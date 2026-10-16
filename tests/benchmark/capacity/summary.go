package main

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"
)

type recoverySummary struct {
	LeaderNodeID           string      `json:"leaderNodeId"`
	KillMarkerStates       []string    `json:"killMarkerStatesObserved"`
	KillTarget             *killTarget `json:"killTarget,omitempty"`
	KillingObservedMillis  float64     `json:"killingObservedMillis"`
	InjectionMillis        float64     `json:"injectionMillis"`
	RTOMillis              float64     `json:"rtoMillis"`
	Recovered              bool        `json:"recovered"`
	AffectedClients        uint64      `json:"affectedClients"`
	SessionPresentFailures uint64      `json:"sessionPresentFailures"`
}

type generatorSummary struct {
	CPUKnown        bool    `json:"cpuKnown"`
	CPUPercentN     float64 `json:"cpuPercentMeasureN"`
	CPUPercentN1    float64 `json:"cpuPercentMeasureN1"`
	CPULimitPercent float64 `json:"cpuSaturationLimitPercent"`
	CPUDenominator  float64 `json:"cpuDenominatorCPUs"`
	PeakRSSBytes    uint64  `json:"peakRssBytes"`
}

type windowSpan struct {
	StartedAt string  `json:"startedAt"`
	Millis    float64 `json:"millis"`
}

type deliverySummary struct {
	gapResult
	QoS1Duplicates        uint64 `json:"qos1Duplicates"`
	QoS2Duplicates        uint64 `json:"qos2Duplicates"`
	UnexpectedDeliveries  uint64 `json:"unexpectedDeliveries"`
	StaleTokenDeliveries  uint64 `json:"staleTokenDeliveries"`
	RetainedReplays       uint64 `json:"retainedReplayDeliveriesIgnored"`
	TenantLimitRejections uint64 `json:"tenantLimitRejections"`
	SetupFailures         uint64 `json:"setupFailures"`
	OutstandingAfterDrain int64  `json:"outstandingExpectedDeliveriesAfterDrain"`
}

// stepSummary is steps/<NN>-r<R>/step-summary.json.
type stepSummary struct {
	SchemaVersion           int                      `json:"schemaVersion"`
	Step                    int                      `json:"step"`
	Run                     int                      `json:"run"`
	Cells                   int                      `json:"cells"`
	RatePerStreamPerSecond  int                      `json:"ratePerStreamPerSecond"`
	OfferedPublishPerSecond int                      `json:"offeredPublishPerSecond"`
	Connections             int                      `json:"connections"`
	TLSClients              int                      `json:"tlsClients"`
	TCPClients              int                      `json:"tcpClients"`
	StartedAt               string                   `json:"startedAt"`
	FinishedAt              string                   `json:"finishedAt"`
	Verdict                 string                   `json:"verdict"`
	Reasons                 []string                 `json:"reasons"`
	FailConditions          []string                 `json:"failConditions"`
	ReleaseGating           bool                     `json:"releaseGating"`
	LatencyAttribution      string                   `json:"latencyAttribution"`
	P99LimitMillis          int64                    `json:"latencyP99LimitMillis"`
	RecoveryLimitMillis     int64                    `json:"recoveryTimeoutMillis"`
	Windows                 map[string]windowSpan    `json:"windows"`
	Counters                map[string]windowStats   `json:"windowCounters"`
	Latency                 map[string][3]qosLatency `json:"latencyByWindow"`
	DeliveryRatesN1         [3]float64               `json:"measureN1FirstCopyDeliveriesPerSecondByQoS"`
	Delivery                deliverySummary          `json:"delivery"`
	Recovery                recoverySummary          `json:"recovery"`
	Generator               generatorSummary         `json:"generator"`
	Markers                 map[string]string        `json:"markerFiles"`
	ActiveClusterFeatures   []string                 `json:"activeClusterFeatures"`
}

type boundsConn struct {
	Connections int     `json:"connections"`
	TLS         int     `json:"tlsClients"`
	TCP         int     `json:"tcpClients"`
	TLSRatio    float64 `json:"tlsRatio"`
	TCPRatio    float64 `json:"tcpRatio"`
}

type boundsPub struct {
	OfferedPerSecond int        `json:"offeredPublishPerSecond"`
	QoSRatio         [3]float64 `json:"qosRatio"`
}

type boundsSub struct {
	DeliveriesPerSecond float64    `json:"measureN1DeliveriesPerSecondMedian"`
	ByQoSPerSecond      [3]float64 `json:"byQoSPerSecond"`
	QoSRatio            [3]float64 `json:"qosRatio"`
	ConfirmRunCount     int        `json:"confirmRuns"`
}

type boundsMsgSize struct {
	P50 int `json:"p50"`
	P95 int `json:"p95"`
	P99 int `json:"p99"`
	Max int `json:"max"`
}

type boundsTopic struct {
	Subscriptions  int `json:"subscriptions"`
	DistinctFilter int `json:"distinctFilters"`
	Exact          int `json:"exactSubscriptions"`
	Wildcard       int `json:"wildcardSubscriptions"`
	Shared         int `json:"sharedSubscriptions"`
}

// capacityBounds are the §3.9 B_* values of the adopted step.
type capacityBounds struct {
	BConn    boundsConn    `json:"B_conn"`
	BPub     boundsPub     `json:"B_pub"`
	BSub     boundsSub     `json:"B_sub"`
	BMsgSize boundsMsgSize `json:"B_msg_size"`
	BTopic   boundsTopic   `json:"B_topic"`
}

// nearestRank returns the nearest-rank percentile of sorted values.
func nearestRank(sorted []int, percentile float64) int {
	rank := int(math.Ceil(percentile / 100 * float64(len(sorted))))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

func qosRatios(p Profile) [3]float64 {
	var counts [3]int
	for _, def := range streamDefs {
		counts[def.qos]++
	}
	var ratios [3]float64
	for qos, count := range counts {
		ratios[qos] = float64(count) / cellStreams
	}
	return ratios
}

func topicBounds(p Profile, cells int) boundsTopic {
	plan := buildCell(p, 0)
	distinct := make(map[string]struct{})
	var bound boundsTopic
	for _, spec := range plan.clients {
		if spec.filter == "" {
			continue
		}
		bound.Subscriptions++
		distinct[spec.filter] = struct{}{}
		switch {
		case strings.HasPrefix(spec.filter, "$share/"):
			bound.Shared++
		case strings.ContainsAny(spec.filter, "+#"):
			bound.Wildcard++
		default:
			bound.Exact++
		}
	}
	bound.DistinctFilter = len(distinct)
	bound.Subscriptions *= cells
	bound.DistinctFilter *= cells
	bound.Exact *= cells
	bound.Wildcard *= cells
	bound.Shared *= cells
	return bound
}

// computeBounds derives B_* from the confirmation runs of the adopted step. It returns nil and a
// reason when no value may be recorded.
func computeBounds(p Profile, outcome capacityOutcome) (*capacityBounds, string) {
	if p.Purpose != purposeEKSCapacity {
		return nil, "purpose " + p.Purpose + " is harness evidence only; B_* are never recorded"
	}
	if outcome.Adopted == nil {
		return nil, outcome.NullReason
	}
	adopted := outcome.Adopted
	var sums []float64
	var byQoS [3][]float64
	for _, run := range outcome.Runs {
		if run.Phase != phaseConfirm || run.Step != adopted.Step || run.summary == nil {
			continue
		}
		var total float64
		for qos, rate := range run.summary.DeliveryRatesN1 {
			byQoS[qos] = append(byQoS[qos], rate)
			total += rate
		}
		sums = append(sums, total)
	}
	if len(sums) == 0 {
		return nil, "the adopted step has no confirmation run summaries"
	}
	tls := countTLSClients(p) * adopted.Cells
	connections := cellClients * adopted.Cells
	sizes := slices.Clone(p.PayloadSizeCycle)
	sort.Ints(sizes)
	bounds := &capacityBounds{
		BConn: boundsConn{
			Connections: connections, TLS: tls, TCP: connections - tls,
			TLSRatio: float64(tls) / float64(connections), TCPRatio: float64(connections-tls) / float64(connections),
		},
		BPub:     boundsPub{OfferedPerSecond: adopted.Offered, QoSRatio: qosRatios(p)},
		BMsgSize: boundsMsgSize{P50: nearestRank(sizes, 50), P95: nearestRank(sizes, 95), P99: nearestRank(sizes, 99), Max: sizes[len(sizes)-1]},
		BTopic:   topicBounds(p, adopted.Cells),
	}
	bounds.BSub.DeliveriesPerSecond = median(sums)
	var total float64
	for qos := range 3 {
		bounds.BSub.ByQoSPerSecond[qos] = median(byQoS[qos])
		total += bounds.BSub.ByQoSPerSecond[qos]
	}
	for qos := range 3 {
		if total > 0 {
			bounds.BSub.QoSRatio[qos] = bounds.BSub.ByQoSPerSecond[qos] / total
		}
	}
	bounds.BSub.ConfirmRunCount = len(sums)
	return bounds, ""
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	sort.Float64s(sorted)
	if len(sorted) == 0 {
		return 0
	}
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

// capacitySummary is capacity-summary.json.
type capacitySummary struct {
	SchemaVersion    int             `json:"schemaVersion"`
	ProfileID        string          `json:"profileId"`
	Purpose          string          `json:"purpose"`
	ProfileSHA256    string          `json:"profileSha256"`
	ReleaseGating    bool            `json:"releaseGating"`
	StartedAt        string          `json:"startedAt"`
	FinishedAt       string          `json:"finishedAt"`
	Ladder           []runRecord     `json:"ladder"`
	Bisect           []runRecord     `json:"bisect"`
	Confirm          []runRecord     `json:"confirm"`
	LadderExhausted  bool            `json:"ladderExhausted"`
	Adopted          *adoptedStep    `json:"adopted"`
	Bounds           *capacityBounds `json:"bounds"`
	BoundsNullReason string          `json:"boundsNullReason,omitempty"`
	Aborted          string          `json:"aborted,omitempty"`
	// ActiveClusterFeatures are the cluster features the runner activated and verified before setup.
	ActiveClusterFeatures []string `json:"activeClusterFeatures"`
}

func buildCapacitySummary(p Profile, sha string, started, finished time.Time, outcome capacityOutcome) capacitySummary {
	summary := capacitySummary{
		SchemaVersion: 1, ProfileID: p.ProfileID, Purpose: p.Purpose, ProfileSHA256: sha, ReleaseGating: false,
		StartedAt: formatTime(started), FinishedAt: formatTime(finished),
		Ladder: []runRecord{}, Bisect: []runRecord{}, Confirm: []runRecord{},
		LadderExhausted: outcome.LadderExhausted, Adopted: outcome.Adopted, Aborted: outcome.Aborted,
	}
	for _, run := range outcome.Runs {
		switch run.Phase {
		case phaseLadder:
			summary.Ladder = append(summary.Ladder, run)
		case phaseBisect:
			summary.Bisect = append(summary.Bisect, run)
		default:
			summary.Confirm = append(summary.Confirm, run)
		}
	}
	bounds, reason := computeBounds(p, outcome)
	summary.Bounds = bounds
	if bounds == nil {
		summary.BoundsNullReason = reason
		if summary.BoundsNullReason == "" {
			summary.BoundsNullReason = "no step was adopted"
		}
	}
	return summary
}

func summaryLine(s capacitySummary) string {
	if s.Adopted != nil {
		return fmt.Sprintf("adopted step %d: %d cells at rate %d (%d msg/s offered)", s.Adopted.Step, s.Adopted.Cells, s.Adopted.Rate, s.Adopted.Offered)
	}
	return "no step adopted: " + s.BoundsNullReason
}
