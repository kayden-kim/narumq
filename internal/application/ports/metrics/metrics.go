package metrics

import "time"

// Protocol labels (fixed cardinality)
const (
	ProtocolMQTT310 = "3.1.0"
	ProtocolMQTT311 = "MQTT_3_1_1"
	ProtocolMQTT50  = "MQTT_5_0"
)

// Connect rejection reasons (fixed cardinality)
const (
	ConnectRejectReasonUnsupportedProtocolVersion = "unsupported_protocol_version"
)

// Transport labels (fixed cardinality)
const (
	TransportTCP = "tcp"
	TransportTLS = "tls"
	TransportWS  = "ws"
	TransportWSS = "wss"
)

// Auth result labels
const (
	AuthResultSuccess = "success"
	AuthResultFailure = "failure"
)

// Auth method labels
const (
	AuthMethodAnonymous = "anonymous"
	AuthMethodPassword  = "password"
	AuthMethodTLS       = "tls_cert"
	AuthMethodJWT       = "jwt"
)

// Will outcome reasons (fixed cardinality).
const (
	WillOutcomeSuppressedNormalDisconnect       = "suppressed_normal_disconnect"
	WillOutcomeSuppressedReconnectedWithinDelay = "suppressed_reconnected_within_delay"
	WillOutcomeExpectedMQTT311ServerDisconnect  = "expected_mqtt311_server_disconnect"
	WillOutcomeExpectedZeroOrExpiredWillDelay   = "expected_zero_or_expired_will_delay"
	WillOutcomeUnexpectedSupportedClient        = "unexpected_supported_client"
	WillOutcomeUnplannedFailure                 = "unplanned_failure"
)

// WillOutcomeRecorder records a registered Will's final outcome.
type WillOutcomeRecorder interface {
	WillOutcome(reason string)
}

// MetricsRecorder records operational metrics for NaruMQ broker.
// Implementations must strictly enforce label cardinality limits:
// client identifiers, IPs, arbitrary topics, or tokens must NEVER be used as metric labels.
type MetricsRecorder interface {
	// ConnectionOpened increments active connections and connection total counter.
	ConnectionOpened(protocol string, transport string)
	// ConnectionClosed decrements active connections and increments closed total.
	ConnectionClosed(protocol string, transport string)

	// PacketReceived increments packet received counter by packet type.
	PacketReceived(packetType string)
	// PacketSent increments packet sent counter by packet type.
	PacketSent(packetType string)

	// AuthAttempt increments authentication counter with method, result and reason.
	AuthAttempt(method string, success bool, reason string)

	// ConnectRejected increments the fixed-cardinality unsupported CONNECT counter.
	ConnectRejected(protocol string, reason string)

	// SetSessionsActive updates the active session gauge for persistent or non-persistent sessions.
	SetSessionsActive(persistent bool, count int64)
	// SetSubscriptionsActive updates the active subscription gauge.
	SetSubscriptionsActive(count int64)

	// SetRetainedStats updates retained messages count and total bytes.
	SetRetainedStats(messageCount int64, totalBytes int64)

	// ObservePublishDuration observes publishing processing duration in seconds.
	ObservePublishDuration(duration time.Duration)
}

// MetricsRenderer renders the registry in Prometheus or OpenMetrics text format.
type MetricsRenderer interface {
	Render(openMetrics bool) []byte
}
