package main

import "testing"

// Traceability: CFG-SCN-46, PRD MQTT-10; docs/specs/configuration-and-runtime-options.md §6.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-46
func TestNodeConfigurationMQTTWebSocketNil(t *testing.T) {
	if got := nodeConfigurationMQTTWebSocket(nil); got != nil {
		t.Fatalf("nodeConfigurationMQTTWebSocket(nil) = %+v, want nil", got)
	}
}
