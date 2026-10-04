package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type lbTraceEvent struct {
	At         string `json:"at"`
	Event      string `json:"event"`
	Backend    string `json:"backend,omitempty"`
	Ready      bool   `json:"ready,omitempty"`
	Connection int    `json:"connection,omitempty"`
}

type fakeServerTraceEvent struct {
	At         string `json:"at"`
	Event      string `json:"event"`
	Listener   string `json:"listener,omitempty"`
	Connection int    `json:"connection,omitempty"`
	ClientID   string `json:"clientId,omitempty"`
	Vector     string `json:"vector,omitempty"`
}

func decodeTrace[T any](path string) ([]T, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	events := make([]T, 0)
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		var event T
		if err := decoder.Decode(&event); err != nil {
			return nil, fmt.Errorf("trace line %d: %w", line, err)
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("trace line %d contains trailing data", line)
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// The caller supplies only the trace prefix observed at driver readiness.
func validateLBInitialConnect(events []lbTraceEvent, drainBackend string) error {
	connects := 0
	for _, event := range events {
		if event.Event != "connect" {
			continue
		}
		connects++
		if event.Backend != drainBackend {
			return fmt.Errorf("initial LB backend %q, want %q", event.Backend, drainBackend)
		}
	}
	if connects != 1 {
		return fmt.Errorf("initial LB connect count %d, want 1", connects)
	}
	return nil
}

func validateFakeServerTrace(events []fakeServerTraceEvent, vector, clientID string) error {
	listeners := map[string]int{"main": 0, "trap": 0}
	connects, vectors := 0, 0
	for _, event := range events {
		switch event.Event {
		case "listening":
			if _, ok := listeners[event.Listener]; !ok {
				return fmt.Errorf("unexpected listener %q", event.Listener)
			}
			listeners[event.Listener]++
		case "connect":
			connects++
			if event.Listener != "main" || event.ClientID != clientID || event.Connection != connects {
				return fmt.Errorf("unexpected fake server connect %+v", event)
			}
			if connects == 2 && vectors != 1 {
				return errors.New("reconnect preceded malformed vector")
			}
		case "vector_sent":
			vectors++
			if event.Connection != 1 || event.Vector != vector || connects != 1 {
				return errors.New("malformed vector did not follow first connection")
			}
		case "trap_accept":
			return errors.New("malformed DISCONNECT caused trap endpoint connection")
		case "connect_invalid":
			return errors.New("fake server rejected invalid CONNECT")
		}
	}
	if listeners["main"] != 1 || listeners["trap"] != 1 {
		return errors.New("fake server must open main and trap listeners once")
	}
	if connects != 2 || vectors != 1 {
		return fmt.Errorf("fake server connects=%d vectors=%d, want 2 and 1", connects, vectors)
	}
	return nil
}
