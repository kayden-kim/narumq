package main

import (
	"strings"
	"sync/atomic"
)

// MQTT 5 reason codes that mean a quota-class limit refused the operation.
const (
	reasonQuotaExceeded          = 0x97
	reasonConnectionRateExceeded = 0x9F
)

// quotaClass is the §3.9 classification of one quota-class rejection.
type quotaClass int

const (
	// quotaTenantLimit: the tenant hit one of its own limits. The step is invalid.
	quotaTenantLimit quotaClass = iota
	// quotaServerBackpressure: the broker refused new publish state to protect itself. The step fails.
	quotaServerBackpressure
	// quotaUnclassified: the packet does not say why. The step fails.
	quotaUnclassified
	numQuotaClasses
)

var quotaClassNames = [numQuotaClasses]string{"tenant_limit", "server_backpressure", "unclassified"}

func (c quotaClass) String() string { return quotaClassNames[c] }

// quotaPacket names the MQTT packet that carried the rejection.
type quotaPacket string

const (
	quotaPacketConnAck quotaPacket = "connack"
	quotaPacketSubAck  quotaPacket = "suback"
	quotaPacketPubAck  quotaPacket = "puback"
	quotaPacketPubRec  quotaPacket = "pubrec"
)

func isQuotaRejection(reason byte) bool {
	return reason == reasonQuotaExceeded || reason == reasonConnectionRateExceeded
}

// classifyQuotaRejection decides the class and cause of one rejection from what the packet
// says. It never guesses: only CONNACK 0x97 (tenant connection and session limits only) and a
// PUBACK/PUBREC 0x97 Reason String "<class>:<cause>" with a known class are classified.
func classifyQuotaRejection(packet quotaPacket, reason byte, reasonString string) (quotaClass, string) {
	if reason != reasonQuotaExceeded {
		return quotaUnclassified, ""
	}
	switch packet {
	case quotaPacketConnAck:
		return quotaTenantLimit, "connections_or_sessions"
	case quotaPacketPubAck, quotaPacketPubRec:
		class, cause, found := strings.Cut(reasonString, ":")
		if !found {
			return quotaUnclassified, ""
		}
		switch class {
		case "tenant_limit":
			switch cause {
			case "payload_size", "message_rate", "byte_rate", "tenant_quota":
				return quotaTenantLimit, cause
			}
		case "server_backpressure":
			switch cause {
			case "commit_lag", "disk_free", "delivery_entries", "delivery_bytes", "delivery_capacity", "receive_capacity", "retained_capacity":
				return quotaServerBackpressure, cause
			}
		}
	}
	return quotaUnclassified, ""
}

// quotaRejectionRecord is one line of quota-rejections.jsonl.
type quotaRejectionRecord struct {
	At             string `json:"at"`
	Phase          string `json:"phase"`
	ClientID       string `json:"clientId"`
	Tenant         string `json:"tenant"`
	Packet         string `json:"packet"`
	ReasonCode     int    `json:"reasonCode"`
	ReasonString   string `json:"reasonString,omitempty"`
	Classification string `json:"classification"`
	Cause          string `json:"cause,omitempty"`
}

// quotaCounters counts rejections per window and class.
type quotaCounters struct {
	counts [numWindows][numQuotaClasses]atomic.Uint64
}

func (q *quotaCounters) add(window windowID, class quotaClass) { q.counts[window][class].Add(1) }

func (q *quotaCounters) total(class quotaClass) uint64 {
	var sum uint64
	for window := range numWindows {
		sum += q.counts[window][class].Load()
	}
	return sum
}

// quotaClassCounts is the per-class count block of step-summary.json.
type quotaClassCounts struct {
	TenantLimit        uint64 `json:"tenantLimit"`
	ServerBackpressure uint64 `json:"serverBackpressure"`
	Unclassified       uint64 `json:"unclassified"`
}

// quotaSummary is the quotaRejections field of step-summary.json.
type quotaSummary struct {
	Total    quotaClassCounts            `json:"total"`
	ByWindow map[string]quotaClassCounts `json:"byWindow"`
}

func (q *quotaCounters) summary() quotaSummary {
	summary := quotaSummary{ByWindow: map[string]quotaClassCounts{}}
	for window := range numWindows {
		counts := quotaClassCounts{
			TenantLimit:        q.counts[window][quotaTenantLimit].Load(),
			ServerBackpressure: q.counts[window][quotaServerBackpressure].Load(),
			Unclassified:       q.counts[window][quotaUnclassified].Load(),
		}
		if counts == (quotaClassCounts{}) {
			continue
		}
		summary.ByWindow[window.String()] = counts
		summary.Total.TenantLimit += counts.TenantLimit
		summary.Total.ServerBackpressure += counts.ServerBackpressure
		summary.Total.Unclassified += counts.Unclassified
	}
	return summary
}
