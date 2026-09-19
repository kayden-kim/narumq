// Package delivery implements deterministic MQTT QoS 1 and QoS 2 inflight
// policy. It performs value-only state transitions and returns protocol actions;
// it does not perform wire I/O, persistence, timers, or goroutine management.
package delivery
