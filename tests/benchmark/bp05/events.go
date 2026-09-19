package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type jsonlWriter struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

func newJSONLWriter(path string) (*jsonlWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	return &jsonlWriter{file: file, writer: bufio.NewWriterSize(file, 64*1024)}, nil
}

func (writer *jsonlWriter) Write(value any) error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if err := json.NewEncoder(writer.writer).Encode(value); err != nil {
		return err
	}
	return writer.writer.Flush()
}

func (writer *jsonlWriter) Close() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.file == nil {
		return nil
	}
	flushErr := writer.writer.Flush()
	closeErr := writer.file.Close()
	writer.file = nil
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

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

func writePhase(writer *jsonlWriter, phase, event string, active int, timestamp time.Time) error {
	return writer.Write(phaseEvent{Phase: phase, Event: event, Timestamp: utcString(timestamp), ActiveConnections: active})
}
