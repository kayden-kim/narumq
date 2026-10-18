package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// Traceability: MQTT-10-SCN-10, CFG-SCN-45/46; docs/specs/mqtt-protocol-and-session.md §1; docs/specs/configuration-and-runtime-options.md §6.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-45, configuration-and-runtime-options.md:CFG-SCN-46, mqtt-protocol-and-session.md:MQTT-10-SCN-10
func TestDynamicClusterWebSocketProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	fixture.webSocketConfigured = true
	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		process, err := fixture.harness.start(
			fmt.Sprintf("WebSocket bootstrap node %d", index+1),
			fixture.bootstrapConfig(uint64(index+1)),
			fixture.managementAddresses[index],
		)
		if err != nil {
			t.Fatalf("start bootstrap node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)

	for _, process := range processes {
		clusterMembershipProcessWaitForStatus(t, fixture.ctx, process, "/health/ready", http.StatusOK)
	}

	subscriber := clusterWebSocketProcessDial(t, fixture.ctx, fixture.webSocketAddresses[1])
	clusterWebSocketProcessConnect(t, subscriber, fixture.accessToken, "live-qos0-subscriber-2")
	const topic = "live/websocket/process-acceptance"
	clusterWebSocketProcessSubscribe(t, subscriber, topic)
	clusterMembershipProcessWaitForRouteProjectionClients(
		t, fixture.ctx, processes, topic, "live-qos0-subscriber-2",
	)

	publisher := clusterWebSocketProcessDial(t, fixture.ctx, fixture.webSocketAddresses[0])
	clusterWebSocketProcessConnect(t, publisher, fixture.accessToken, "live-qos0-publisher-1")
	const payload = "node-1-to-node-2-over-websocket"
	publishBody := clusterWebSocketProcessString(nil, topic)
	publishBody = append(publishBody, payload...)
	clusterWebSocketProcessWritePacket(t, publisher, 0x30, publishBody)

	got := clusterWebSocketProcessReadPublish(t, subscriber)
	if got != [2]string{topic, payload} {
		t.Fatalf("WebSocket subscriber publish = %v, want topic=%q payload=%q", got, topic, payload)
	}

	requestHeader := make(http.Header)
	requestHeader.Set("Authorization", "Bearer "+clusterMembershipProcessAdmin)
	status, body, err := clusterMembershipProcessRequestWithHeader(
		fixture.ctx, http.MethodGet, processes[0].managementURL()+"/api/v1/nodes/1/config", nil, requestHeader,
	)
	if err != nil || status != http.StatusOK {
		t.Fatalf("get applied node configuration status=%d body=%s err=%v", status, body, err)
	}
	var response struct {
		Data struct {
			MQTT struct {
				WebSocket *struct {
					ListenAddress    string `json:"listenAddress"`
					TLSListenAddress string `json:"tlsListenAddress"`
				} `json:"webSocket"`
			} `json:"mqtt"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode applied node configuration: %v; body=%s", err, body)
	}
	if response.Data.MQTT.WebSocket == nil {
		t.Fatalf("applied MQTT configuration has no webSocket object: %s", body)
	}
	if got, want := response.Data.MQTT.WebSocket.ListenAddress, fixture.webSocketAddresses[0]; got != want {
		t.Errorf("applied mqtt.webSocket.listenAddress = %q, want %q", got, want)
	}
	if got := response.Data.MQTT.WebSocket.TLSListenAddress; got != "" {
		t.Errorf("applied mqtt.webSocket.tlsListenAddress = %q, want empty", got)
	}

	invalidConfigPath := filepath.Join(fixture.tempDir, "invalid-wss-config.json")
	invalidConfig := clusterMembershipProcessConfig{
		Mode: "cluster", ClusterID: clusterMembershipProcessClusterID, NodeID: 1,
		Directory:     filepath.Join(fixture.tempDir, "invalid-wss-data"),
		ListenAddress: fixture.peerAddresses[0], AdvertisedAddress: fixture.peerAddresses[0],
		LocalDNSName:         fixture.certificates[0].dnsName,
		ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
		Role: "bootstrap", BootstrapMembers: append([]clusterMembershipProcessMember(nil), fixture.bootstrapMembers...),
		SeedAddresses: []string{}, PeerTLS: clusterMembershipProcessTLS{
			CertFile: fixture.certificates[0].certPath(fixture.tempDir),
			KeyFile:  fixture.certificates[0].keyPath(fixture.tempDir), CAFile: fixture.caPath,
		},
		Management: fixture.managementConfig(0, 1),
		MQTT: clusterMembershipProcessMQTT{
			TCPListenAddress: fixture.mqttAddresses[0], SecurityConfigFile: fixture.securityPath,
			WebSocket: &clusterMembershipProcessMQTTWebSocket{TLSListenAddress: fixture.webSocketAddresses[2]},
		},
	}
	clusterMembershipProcessWriteJSON(t, invalidConfigPath, invalidConfig)
	invalidProcess, err := fixture.harness.start("WSS without TLS configuration", invalidConfigPath, fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("start invalid WSS process: %v", err)
	}
	select {
	case <-invalidProcess.done:
	case <-time.After(15 * time.Second):
		t.Fatal("invalid WSS process did not exit")
	}
	if exitCode, exited := invalidProcess.exitCode(); !exited || exitCode == 0 {
		t.Fatalf("invalid WSS process exit = (%d, %t), want nonzero; output:\n%s", exitCode, exited, invalidProcess.logs.String())
	}
	const wantError = "mqtt tls is required when tlsListenAddress or webSocket.tlsListenAddress is set"
	if output := invalidProcess.logs.String(); !strings.Contains(output, wantError) {
		t.Fatalf("invalid WSS process output does not contain %q:\n%s", wantError, output)
	}
}

func clusterWebSocketProcessDial(t *testing.T, ctx context.Context, address string) net.Conn {
	t.Helper()
	connection, _, err := coderws.Dial(ctx, "ws://"+address+"/mqtt", &coderws.DialOptions{Subprotocols: []string{"mqtt"}})
	if err != nil {
		t.Fatalf("dial MQTT WebSocket %s: %v", address, err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	return coderws.NetConn(context.Background(), connection, coderws.MessageBinary)
}

func clusterWebSocketProcessConnect(t *testing.T, connection net.Conn, token, clientID string) {
	t.Helper()
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xC2, 0, 60}
	body = clusterWebSocketProcessString(body, clientID)
	body = clusterWebSocketProcessString(body, "jwt")
	body = clusterWebSocketProcessString(body, token)
	clusterWebSocketProcessWritePacket(t, connection, 0x10, body)
	header, response, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil || header != 0x20 || !bytes.Equal(response, []byte{0, 0}) {
		t.Fatalf("WebSocket CONNECT response=(0x%02x, % X), err=%v", header, response, err)
	}
}

func clusterWebSocketProcessSubscribe(t *testing.T, connection net.Conn, topic string) {
	t.Helper()
	body := []byte{0, 1}
	body = clusterWebSocketProcessString(body, topic)
	body = append(body, 0)
	clusterWebSocketProcessWritePacket(t, connection, 0x82, body)
	header, response, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil || header != 0x90 || len(response) < 3 || response[len(response)-1] != 0 {
		t.Fatalf("WebSocket SUBSCRIBE response=(0x%02x, % X), err=%v", header, response, err)
	}
}

func clusterWebSocketProcessReadPublish(t *testing.T, connection net.Conn) [2]string {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set MQTT WebSocket read deadline: %v", err)
	}
	header, body, err := clusterMembershipProcessReadMQTTPacket(connection)
	if err != nil {
		t.Fatalf("read MQTT WebSocket PUBLISH: %v", err)
	}
	if header != 0x30 || len(body) < 2 {
		t.Fatalf("WebSocket PUBLISH header/body = 0x%02x/%x, want QoS 0", header, body)
	}
	topicLength := int(body[0])<<8 | int(body[1])
	if topicLength == 0 || topicLength > len(body)-2 {
		t.Fatalf("WebSocket PUBLISH topic length %d exceeds body length %d", topicLength, len(body))
	}
	return [2]string{string(body[2 : 2+topicLength]), string(body[2+topicLength:])}
}

func clusterWebSocketProcessWritePacket(t *testing.T, connection net.Conn, header byte, body []byte) {
	t.Helper()
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set MQTT WebSocket packet deadline: %v", err)
	}
	if _, err := connection.Write(clusterMembershipProcessEncodeMQTTPacket(header, body)); err != nil {
		t.Fatalf("write MQTT WebSocket packet 0x%02x: %v", header, err)
	}
}

func clusterWebSocketProcessString(body []byte, value string) []byte {
	body = append(body, byte(len(value)>>8), byte(len(value)))
	return append(body, value...)
}
