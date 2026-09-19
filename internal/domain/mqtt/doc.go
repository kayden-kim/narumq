// Package mqtt implements the network-independent MQTT packet codec used by
// NaruMQ's domain layer.
//
// The package currently supports the MQTT 3.1.1 and MQTT 5.0 CONNECT,
// CONNACK, and QoS 0 PUBLISH packet subset defined by PRD MQTT-01, MQTT-11,
// MQTT-12, and MQTT-13. Decode borrows its input only while the call runs;
// every returned packet owns its strings and byte slices.
package mqtt
