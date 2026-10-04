package main

import (
	"os"
	"path/filepath"
	"testing"
)

func validFakeTrace() []fakeServerTraceEvent {
	return []fakeServerTraceEvent{
		{Event: "listening", Listener: "main"}, {Event: "listening", Listener: "trap"},
		{Event: "connect", Listener: "main", Connection: 1, ClientID: "client"},
		{Event: "vector_sent", Connection: 1, Vector: "truncated_property"},
		{Event: "connect", Listener: "main", Connection: 2, ClientID: "client"},
	}
}

func TestValidateFakeServerTraceRejectsTrapConnection(t *testing.T) {
	events := validFakeTrace()
	if err := validateFakeServerTrace(events, "truncated_property", "client"); err != nil {
		t.Fatal(err)
	}
	events = append(events, fakeServerTraceEvent{Event: "trap_accept", Listener: "trap"})
	if err := validateFakeServerTrace(events, "truncated_property", "client"); err == nil {
		t.Fatal("accepted trap connection")
	}
}

func TestValidateFakeServerTraceRequiresSingleVectorThenReconnect(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]fakeServerTraceEvent) []fakeServerTraceEvent
	}{
		{"missing vector", func(e []fakeServerTraceEvent) []fakeServerTraceEvent { return append(e[:3], e[4:]...) }},
		{"duplicate vector", func(e []fakeServerTraceEvent) []fakeServerTraceEvent { return append(e, e[3]) }},
		{"reconnect before vector", func(e []fakeServerTraceEvent) []fakeServerTraceEvent { e[3], e[4] = e[4], e[3]; return e }},
		{"wrong identity", func(e []fakeServerTraceEvent) []fakeServerTraceEvent { e[4].ClientID = "other"; return e }},
		{"missing reconnect", func(e []fakeServerTraceEvent) []fakeServerTraceEvent { return e[:4] }},
		{"wrong vector", func(e []fakeServerTraceEvent) []fakeServerTraceEvent { e[3].Vector = "other"; return e }},
		{"missing trap listener", func(e []fakeServerTraceEvent) []fakeServerTraceEvent { return append(e[:1], e[2:]...) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateFakeServerTrace(test.mutate(validFakeTrace()), "truncated_property", "client"); err == nil {
				t.Fatal("accepted invalid trace")
			}
		})
	}
}

func TestValidateLBInitialConnectRequiresDrainNode(t *testing.T) {
	const backend = "node3.compat.test:1883"
	if err := validateLBInitialConnect([]lbTraceEvent{{Event: "health", Backend: backend, Ready: true}, {Event: "connect", Backend: backend}}, backend); err != nil {
		t.Fatal(err)
	}
	for _, events := range [][]lbTraceEvent{nil, {{Event: "connect", Backend: "node1.compat.test:1883"}}, {{Event: "connect", Backend: backend}, {Event: "connect", Backend: backend}}} {
		if err := validateLBInitialConnect(events, backend); err == nil {
			t.Fatal("accepted invalid initial connection")
		}
	}
}

func TestDecodeDrainTraceRejectsUnknownFieldsAndTrailingData(t *testing.T) {
	for _, line := range []string{`{"event":"connect","unknown":true}`, `{"event":"connect"} {"event":"reject"}`} {
		path := filepath.Join(t.TempDir(), "trace.jsonl")
		if err := os.WriteFile(path, []byte(line+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := decodeTrace[lbTraceEvent](path); err == nil {
			t.Fatal("accepted invalid trace JSON")
		}
	}
}
