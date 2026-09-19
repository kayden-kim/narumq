package main

import (
	"time"

	"narumq/tests/benchmark/internal/jsonl"
)

type connectionEvent struct {
	ClientID               string `json:"clientId"`
	Phase                  string `json:"phase"`
	ScheduledAt            string `json:"scheduledAt"`
	StartedAt              string `json:"startedAt"`
	ConnAckAt              string `json:"connAckAt"`
	ScheduleLagNanoseconds int64  `json:"scheduleLagNanoseconds"`
	LatencyNanoseconds     int64  `json:"latencyNanoseconds"`
	Success                bool   `json:"success"`
	Error                  string `json:"error"`
}

type phaseEvent struct {
	Phase             string `json:"phase"`
	Event             string `json:"event"`
	Timestamp         string `json:"timestamp"`
	ActiveConnections int    `json:"activeConnections"`
}

func utcString(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func writePhase(writer *jsonl.Writer, phase, event string, active int, timestamp time.Time) error {
	return writer.Write(phaseEvent{Phase: phase, Event: event, Timestamp: utcString(timestamp), ActiveConnections: active})
}
