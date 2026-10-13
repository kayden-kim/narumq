package tcp

import (
	"net"
	"testing"
	"time"

	portsmetrics "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/mqtt"
)

type labeledTestConn struct {
	net.Conn
}

func (labeledTestConn) MetricsTransport() string {
	return portsmetrics.TransportWS
}

type connectionMetricEvent struct {
	opened    bool
	protocol  string
	transport string
}

type connectionMetricsRecorder struct {
	events chan connectionMetricEvent
}

func (recorder *connectionMetricsRecorder) ConnectionOpened(protocol, transport string) {
	recorder.events <- connectionMetricEvent{opened: true, protocol: protocol, transport: transport}
}

func (recorder *connectionMetricsRecorder) ConnectionClosed(protocol, transport string) {
	recorder.events <- connectionMetricEvent{protocol: protocol, transport: transport}
}

func (*connectionMetricsRecorder) PacketReceived(string)                {}
func (*connectionMetricsRecorder) PacketSent(string)                    {}
func (*connectionMetricsRecorder) AuthAttempt(string, bool, string)     {}
func (*connectionMetricsRecorder) ConnectRejected(string, string)       {}
func (*connectionMetricsRecorder) MessageDropped(string)                {}
func (*connectionMetricsRecorder) SetSessionsActive(bool, int64)        {}
func (*connectionMetricsRecorder) SetRetainedStats(int64, int64)        {}
func (*connectionMetricsRecorder) ObservePublishDuration(time.Duration) {}

// Traceability: PRD MQTT-10; docs/specs/mqtt-protocol-and-session.md §1, §7.1.
func TestServerConnectionMetricsUseSocketTransportLabel(t *testing.T) {
	recorder := &connectionMetricsRecorder{events: make(chan connectionMetricEvent, 2)}
	config := defaultAdapterConfig()
	config.Metrics = recorder
	runtime := newAdapterTestRuntime(t, 2, config)
	defer runtime.stop(t)
	if err := runtime.client.Close(); err != nil {
		t.Fatal(err)
	}

	listener := newPipeListener()
	if err := runtime.server.AddListener(listener); err != nil {
		t.Fatal(err)
	}
	client, socket := net.Pipe()
	defer client.Close()
	listener.connections <- labeledTestConn{Conn: socket}
	select {
	case <-listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept labeled connection")
	}

	writeTestPackets(t, client, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "ws-metrics",
	})
	if frame := readTestFrame(t, client); len(frame) < 4 || frame[0] != 0x20 || frame[3] != 0 {
		t.Fatalf("CONNACK = %x, want successful CONNACK", frame)
	}
	select {
	case event := <-recorder.events:
		if !event.opened || event.protocol != portsmetrics.ProtocolMQTT50 || event.transport != portsmetrics.TransportWS {
			t.Fatalf("opened event = %#v, want MQTT 5 over ws", event)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not record opened connection")
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-recorder.events:
		if event.opened || event.protocol != portsmetrics.ProtocolMQTT50 || event.transport != portsmetrics.TransportWS {
			t.Fatalf("closed event = %#v, want MQTT 5 over ws", event)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not record closed connection")
	}
}
