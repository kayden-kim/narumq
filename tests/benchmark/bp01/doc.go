// Package main implements the BP-01 MQTT 5 QoS 0 protocol baseline harness.
//
// It is an external workload and Docker orchestration tool. Product runtime
// packages must not import it; the runner deliberately reuses the domain MQTT
// encoder and decoder at the benchmark boundary.
package main
