package tcp

import (
	"testing"

	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
)

// Traceability: UI-03; docs/specs/management-api-and-observability.md §7.1.
func TestConnectRejectReasonMapping(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		version      mqtt.ProtocolVersion
		reasonCode   byte
		wantProtocol string
		wantReason   string
	}{
		{name: "MQTT 3.1.1 unsupported version", version: mqtt.ProtocolV311, reasonCode: 0x01, wantProtocol: portsmetrics.ProtocolMQTT311, wantReason: portsmetrics.ConnectRejectReasonUnsupportedProtocolVersion},
		{name: "MQTT 3.1.1 identifier rejected", version: mqtt.ProtocolV311, reasonCode: 0x02, wantProtocol: portsmetrics.ProtocolMQTT311, wantReason: portsmetrics.ConnectRejectReasonIdentifierRejected},
		{name: "MQTT 3.1.1 server unavailable", version: mqtt.ProtocolV311, reasonCode: 0x03, wantProtocol: portsmetrics.ProtocolMQTT311, wantReason: portsmetrics.ConnectRejectReasonServerUnavailable},
		{name: "MQTT 3.1.1 bad credentials", version: mqtt.ProtocolV311, reasonCode: 0x04, wantProtocol: portsmetrics.ProtocolMQTT311, wantReason: portsmetrics.ConnectRejectReasonBadCredentials},
		{name: "MQTT 3.1.1 not authorized", version: mqtt.ProtocolV311, reasonCode: 0x05, wantProtocol: portsmetrics.ProtocolMQTT311, wantReason: portsmetrics.ConnectRejectReasonNotAuthorized},
		{name: "MQTT 3.1.1 unknown reason", version: mqtt.ProtocolV311, reasonCode: 0x06, wantProtocol: portsmetrics.ProtocolMQTT311, wantReason: portsmetrics.ConnectRejectReasonOther},
		{name: "MQTT 5 unsupported version", version: mqtt.ProtocolV5, reasonCode: 0x84, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonUnsupportedProtocolVersion},
		{name: "MQTT 5 identifier rejected", version: mqtt.ProtocolV5, reasonCode: 0x85, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonIdentifierRejected},
		{name: "MQTT 5 bad credentials", version: mqtt.ProtocolV5, reasonCode: 0x86, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonBadCredentials},
		{name: "MQTT 5 not authorized", version: mqtt.ProtocolV5, reasonCode: 0x87, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonNotAuthorized},
		{name: "MQTT 5 server unavailable", version: mqtt.ProtocolV5, reasonCode: 0x88, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonServerUnavailable},
		{name: "MQTT 5 server busy", version: mqtt.ProtocolV5, reasonCode: 0x89, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonServerUnavailable},
		{name: "MQTT 5 quota exceeded", version: mqtt.ProtocolV5, reasonCode: 0x97, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonQuotaExceeded},
		{name: "MQTT 5 unknown reason", version: mqtt.ProtocolV5, reasonCode: 0x80, wantProtocol: portsmetrics.ProtocolMQTT50, wantReason: portsmetrics.ConnectRejectReasonOther},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := protocolMetricLabel(testCase.version); got != testCase.wantProtocol {
				t.Fatalf("protocolMetricLabel() = %q, want %q", got, testCase.wantProtocol)
			}
			if got := connectRejectReason(testCase.version, testCase.reasonCode); got != testCase.wantReason {
				t.Fatalf("connectRejectReason(%#x) = %q, want %q", testCase.reasonCode, got, testCase.wantReason)
			}
		})
	}
}
