// Package mqtt implements the network-independent MQTT packet codec used by
// NaruMQ's domain layer.
//
// The package supports the MQTT 3.1.1 and MQTT 5.0 CONNECT, CONNACK, QoS 0/1/2
// PUBLISH, PUBACK, PUBREC, PUBREL, and PUBCOMP packet contracts required by PRD
// MQTT-01, MQTT-02, MQTT-03, MQTT-11, MQTT-12, and MQTT-13. MQTT 5 CONNECT
// supports the Session Expiry Interval property. Decode borrows its input only
// while the call runs; every returned packet owns its strings and byte slices.
package mqtt
