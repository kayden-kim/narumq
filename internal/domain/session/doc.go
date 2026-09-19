// Package session implements the deterministic MQTT Session Expiry lifecycle.
// It receives caller-provided timestamps and returns value transitions; it does
// not read the wall clock, create timers, sleep, or persist state.
package session
