// Package mqtt implements the network-independent MQTT packet codec used by
// NaruMQ's domain layer.
//
// The package supports the MQTT 3.1.1 and MQTT 5.0 CONNECT, CONNACK, QoS 0/1/2
// PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE,
// and UNSUBACK packet contracts required by PRD MQTT-01, MQTT-02, MQTT-03,
// MQTT-06, MQTT-08 (wire model only; runtime options are deferred), MQTT-11,
// MQTT-12, and MQTT-13. MQTT 5 CONNECT supports the Session Expiry Interval
// property; subscription packets preserve MQTT 5 option and reason-code fields
// on the wire while using the current property-free profile. Decode borrows its
// input only while the call runs; every returned packet owns its strings and
// byte slices.
package mqtt
