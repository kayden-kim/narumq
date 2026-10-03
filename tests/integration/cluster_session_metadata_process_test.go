package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

// Traceability: CLU-08; docs/specs/cluster-session-and-delivery.md §4.3, §11.24.
func TestDynamicClusterSessionMetadataProcessAcceptance(t *testing.T) {
	fixture, nodes := clusterDurableProcessStart(t)
	ctx := fixture.ctx

	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodPost,
		nodes[0].managementURL()+"/api/v1/cluster/features/session-metadata", nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("activate session-metadata status=%d body=%s err=%v", status, body, err)
	}
	clusterMembershipProcessWait(t, ctx, "session-metadata active on every node", func() (bool, string, error) {
		for _, node := range nodes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				node.managementURL()+"/api/v1/cluster/features", nil, clusterMembershipProcessAdmin, "")
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("%s feature status=%d body=%s err=%v", node.name, status, body, err), nil
			}
			var envelope struct {
				Data struct {
					ActiveFeatures []string `json:"activeFeatures"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &envelope); err != nil {
				return false, "decode feature state: " + err.Error(), nil
			}
			found := false
			for _, feature := range envelope.Data.ActiveFeatures {
				found = found || feature == "session-metadata"
			}
			if !found {
				return false, node.name + " has not applied session-metadata", nil
			}
		}
		return true, "all nodes report session-metadata", nil
	})

	observer := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "meta-observer", true)
	defer observer.Close()
	clusterDurableProcessSubscribe(t, observer, 1, "meta/will/#", 0)

	crashSession := clusterSessionMetadataConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "meta-crash", 6,
		&clusterSessionMetadataWill{topic: "meta/will/crash", payload: "crash", delay: 2})
	resumeSession := clusterSessionMetadataConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "meta-resume", 300, nil)
	clusterSessionMetadataSubscribe(t, crashSession, 31, "meta/data/#")
	clusterSessionMetadataSubscribe(t, resumeSession, 32, "meta/data/#")
	takeoverSession := clusterSessionMetadataConnect(t, fixture.mqttAddresses[2], fixture.accessToken, "meta-takeover", 60,
		&clusterSessionMetadataWill{topic: "meta/will/takeover", payload: "takeover"})
	clusterSessionMetadataWaitForSubscriptions(t, ctx, nodes)

	if err := nodes[2].crash(5 * time.Second); err != nil {
		t.Fatalf("crash node 3: %v", err)
	}
	clusterSessionMetadataAssertNoPublish(t, observer, 3*time.Second)
	client := clusterSessionMetadataGetClient(t, ctx, nodes[0], "meta-crash")
	if !client.Connected || client.NodeID != "3" || client.Protocol != "MQTT_5_0" {
		t.Fatalf("crashed owner metadata=%+v, want connected node 3 MQTT_5_0", client)
	}

	_ = takeoverSession.Close()
	takeoverSession = clusterSessionMetadataConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "meta-takeover", 60, nil)
	if !clusterDurableProcessSessionPresent(t, takeoverSession) {
		t.Fatal("takeover CONNACK returned SessionPresent=false")
	}
	publisher := clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "meta-publisher", true)
	defer publisher.Close()
	clusterDurableProcessPublish(t, publisher, "meta/data/1", 1, 71, "queued-during-owner-loss")
	clusterDurableProcessAck(t, publisher, 4, 71)

	restarted, err := fixture.harness.start("session metadata node 3 restart", fixture.bootstrapConfig(3), fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3: %v", err)
	}
	nodes[2] = restarted
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	will := clusterSessionMetadataReadPublish(t, observer, 10*time.Second, false)
	if will.topic != "meta/will/crash" || will.payload != "crash" {
		t.Fatalf("recovered Will=%+v, want meta/will/crash exactly once", will)
	}
	client = clusterSessionMetadataGetClient(t, ctx, nodes[0], "meta-crash")
	if client.Connected || client.ExpiryAt == nil || client.OfflineQueueCount < 1 {
		t.Fatalf("recovered disconnected metadata=%+v, want expiry and offline queue", client)
	}

	_ = resumeSession.Close()
	resumeSession = clusterSessionMetadataConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "meta-resume", 300, nil)
	defer resumeSession.Close()
	if !clusterDurableProcessSessionPresent(t, resumeSession) {
		t.Fatal("meta-resume CONNACK returned SessionPresent=false")
	}
	message := clusterSessionMetadataReadPublish(t, resumeSession, 5*time.Second, true)
	if message.topic != "meta/data/1" || message.payload != "queued-during-owner-loss" || message.qos != 1 || message.packetID == 0 {
		t.Fatalf("resumed durable delivery=%+v", message)
	}
	clusterSessionMetadataAck(t, resumeSession, 0x40, message.packetID)
	clusterSessionMetadataAssertNoPublish(t, observer, time.Second)

	expiryCtx, cancelExpiry := context.WithTimeout(ctx, 20*time.Second)
	defer cancelExpiry()
	clusterMembershipProcessWait(t, expiryCtx, "expired metadata removed from every node", func() (bool, string, error) {
		for _, node := range nodes {
			status, body, err := clusterMembershipProcessRequest(expiryCtx, http.MethodGet,
				node.managementURL()+"/api/v1/clients/acceptance/meta-crash", nil, clusterMembershipProcessAdmin, "")
			if err != nil {
				return false, err.Error(), nil
			}
			if status != http.StatusNotFound {
				return false, fmt.Sprintf("%s client status=%d body=%s", node.name, status, body), nil
			}
		}
		return true, "metadata expired on all nodes", nil
	})
	_ = crashSession.Close()
	expiredResume := clusterSessionMetadataConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "meta-crash", 6, nil)
	defer expiredResume.Close()
	if clusterDurableProcessSessionPresent(t, expiredResume) {
		t.Fatal("expired session reconnected with SessionPresent=true")
	}
	clusterSessionMetadataAssertNoPublish(t, expiredResume, time.Second)

	// Close transport without MQTT DISCONNECT, then restart its owner so recovery
	// must re-arm the committed absolute Will deadline rather than restart delay.
	delayed := clusterSessionMetadataConnect(t, fixture.mqttAddresses[1], fixture.accessToken, "meta-delayed", 60,
		&clusterSessionMetadataWill{topic: "meta/will/delayed", payload: "delayed", delay: 6})
	if err := delayed.Close(); err != nil {
		t.Fatalf("abruptly close delayed-Will session: %v", err)
	}
	delayedAt := time.Now()
	time.Sleep(time.Second)
	if err := nodes[1].crash(5 * time.Second); err != nil {
		t.Fatalf("crash node 2 after abrupt client close: %v", err)
	}
	restarted, err = fixture.harness.start("session metadata node 2 restart", fixture.bootstrapConfig(2), fixture.managementAddresses[1])
	if err != nil {
		t.Fatalf("restart node 2: %v", err)
	}
	nodes[1] = restarted
	clusterMembershipProcessWaitForStatus(t, ctx, restarted, "/health/ready", http.StatusOK)
	silenceRemaining := time.Until(delayedAt.Add(5500 * time.Millisecond))
	if silenceRemaining <= 0 {
		t.Fatal("node restart exceeded delayed-Will pre-deadline observation window")
	}
	clusterSessionMetadataAssertNoPublish(t, observer, silenceRemaining)
	delayedWill := clusterSessionMetadataReadPublish(t, observer, time.Until(delayedAt.Add(30*time.Second)), false)
	if delayedWill.topic != "meta/will/delayed" || delayedWill.payload != "delayed" {
		t.Fatalf("delayed recovered Will=%+v", delayedWill)
	}
	clusterSessionMetadataAssertNoPublish(t, observer, 3*time.Second)
}

type clusterSessionMetadataWill struct {
	topic, payload string
	delay          uint32
}

type clusterSessionMetadataPacket struct {
	kind           byte
	qos            byte
	packetID       uint16
	topic, payload string
}

type clusterSessionMetadataClient struct {
	Connected         bool       `json:"connected"`
	NodeID            string     `json:"nodeId"`
	Protocol          string     `json:"protocol"`
	ExpiryAt          *time.Time `json:"expiryAt"`
	OfflineQueueCount int        `json:"offlineQueueCount"`
}

func clusterSessionMetadataConnect(t *testing.T, address, token, clientID string, expiry uint32, will *clusterSessionMetadataWill) net.Conn {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
		flags := byte(0xC0) // Username and Password carry the fixture JWT credential.
		if will != nil {
			flags |= 0x04
		}
		body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, flags, 0, 60}
		properties := []byte{0x11, byte(expiry >> 24), byte(expiry >> 16), byte(expiry >> 8), byte(expiry)}
		body = append(body, clusterSessionMetadataVarInt(uint32(len(properties)))...)
		body = append(body, properties...)
		body = append(body, byte(len(clientID)>>8), byte(len(clientID)))
		body = append(body, clientID...)
		if will != nil {
			willProperties := []byte{0x18, 0, 0, 0, byte(will.delay)}
			body = append(body, clusterSessionMetadataVarInt(uint32(len(willProperties)))...)
			body = append(body, willProperties...)
			body = append(body, byte(len(will.topic)>>8), byte(len(will.topic)))
			body = append(body, will.topic...)
			body = append(body, byte(len(will.payload)>>8), byte(len(will.payload)))
			body = append(body, will.payload...)
		}
		for _, field := range []string{"jwt", token} {
			body = append(body, byte(len(field)>>8), byte(len(field)))
			body = append(body, field...)
		}
		if err := clusterDurableProcessWrite(conn, 0x10, body); err != nil {
			lastErr = err
		} else {
			kind, _, ack, readErr := clusterDurableProcessReadRaw(conn)
			if readErr == nil && kind == 2 && len(ack) >= 3 && ack[1] == 0 {
				present := ack[0]&1 != 0
				if _, consumed := clusterSessionMetadataReadVarInt(ack[2:]); consumed > 0 {
					_ = conn.SetDeadline(time.Time{})
					return clusterDurableProcessWrap(conn, present)
				}
			} else {
				lastErr = fmt.Errorf("unexpected MQTT 5 CONNACK kind=%d body=%x err=%v", kind, ack, readErr)
			}
		}
		_ = conn.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("MQTT 5 CONNECT %q to %s timed out: %v", clientID, address, lastErr)
	return nil
}

func clusterSessionMetadataVarInt(value uint32) []byte {
	encoded := make([]byte, 0, 4)
	for {
		digit := byte(value % 128)
		value /= 128
		if value > 0 {
			digit |= 0x80
		}
		encoded = append(encoded, digit)
		if value == 0 {
			return encoded
		}
	}
}

func clusterSessionMetadataReadVarInt(body []byte) (int, int) {
	value, multiplier := 0, 1
	for index, digit := range body {
		value += int(digit&127) * multiplier
		if digit&128 == 0 {
			return value, index + 1
		}
		multiplier *= 128
	}
	return 0, 0
}

func clusterSessionMetadataSubscribe(t *testing.T, conn net.Conn, id uint16, filter string) {
	t.Helper()
	body := []byte{byte(id >> 8), byte(id), 0, byte(len(filter) >> 8), byte(len(filter))}
	body = append(body, filter...)
	body = append(body, 1)
	if err := clusterDurableProcessWrite(conn, 0x82, body); err != nil {
		t.Fatal(err)
	}
	kind, _, ack, err := clusterDurableProcessReadRaw(conn)
	if err != nil || kind != 9 || len(ack) < 4 || binary.BigEndian.Uint16(ack[:2]) != id || ack[3] != 1 {
		t.Fatalf("MQTT 5 SUBACK kind=%d body=%x err=%v", kind, ack, err)
	}
}

func clusterSessionMetadataReadPublish(t *testing.T, conn net.Conn, timeout time.Duration, mqtt5 bool) clusterSessionMetadataPacket {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})
	kind, flags, body, err := clusterDurableProcessReadRaw(conn)
	if err != nil {
		t.Fatalf("read MQTT delivery: %v", err)
	}
	packet := clusterSessionMetadataPacket{kind: kind, qos: (flags >> 1) & 3}
	if kind != 3 || len(body) < 2 {
		t.Fatalf("expected MQTT PUBLISH, kind=%d body=%x", kind, body)
	}
	topicLength := int(binary.BigEndian.Uint16(body[:2]))
	if len(body) < 2+topicLength {
		t.Fatalf("short MQTT PUBLISH topic: %x", body)
	}
	packet.topic = string(body[2 : 2+topicLength])
	offset := 2 + topicLength
	if packet.qos > 0 {
		if len(body) < offset+2 {
			t.Fatalf("short MQTT PUBLISH packet ID: %x", body)
		}
		packet.packetID = binary.BigEndian.Uint16(body[offset:])
		offset += 2
	}
	if mqtt5 {
		propertiesLength, consumed := clusterSessionMetadataReadVarInt(body[offset:])
		if consumed == 0 || len(body) < offset+consumed+propertiesLength {
			t.Fatalf("invalid MQTT 5 PUBLISH properties: %x", body[offset:])
		}
		offset += consumed + propertiesLength
	}
	packet.payload = string(body[offset:])
	return packet
}

func clusterSessionMetadataAck(t *testing.T, conn net.Conn, kind byte, id uint16) {
	t.Helper()
	if err := clusterDurableProcessWrite(conn, kind, []byte{byte(id >> 8), byte(id), 0, 0}); err != nil {
		t.Fatal(err)
	}
}

func clusterSessionMetadataAssertNoPublish(t *testing.T, conn net.Conn, duration time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(duration))
	_, _, _, err := clusterDurableProcessReadRaw(conn)
	_ = conn.SetReadDeadline(time.Time{})
	if err == nil {
		t.Fatal("unexpected MQTT delivery")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("waiting for no MQTT delivery: %v", err)
	}
}

func clusterSessionMetadataGetClient(t *testing.T, ctx context.Context, node *clusterMembershipProcess, clientID string) clusterSessionMetadataClient {
	t.Helper()
	status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
		node.managementURL()+"/api/v1/clients/acceptance/"+clientID, nil, clusterMembershipProcessAdmin, "")
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET client %s status=%d body=%s err=%v", clientID, status, body, err)
	}
	var envelope struct {
		Data clusterSessionMetadataClient `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode client %s: %v body=%s", clientID, err, body)
	}
	return envelope.Data
}

func clusterSessionMetadataWaitForSubscriptions(t *testing.T, ctx context.Context, nodes []*clusterMembershipProcess) {
	t.Helper()
	want := map[string]string{"meta-observer": "meta/will/#", "meta-crash": "meta/data/#", "meta-resume": "meta/data/#"}
	clusterMembershipProcessWait(t, ctx, "metadata session subscriptions applied on every node", func() (bool, string, error) {
		for _, node := range nodes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				node.managementURL()+"/api/v1/subscriptions?tenantId=acceptance", nil, clusterMembershipProcessAdmin, "")
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("subscriptions status=%d err=%v", status, err), nil
			}
			var envelope struct {
				Data []struct {
					ClientID    string `json:"clientId"`
					TopicFilter string `json:"topicFilter"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &envelope); err != nil {
				return false, "", err
			}
			if len(envelope.Data) != len(want) {
				return false, fmt.Sprintf("%s subscriptions=%d want=%d", node.name, len(envelope.Data), len(want)), nil
			}
			seen := make(map[string]struct{}, len(want))
			for _, row := range envelope.Data {
				if filter, found := want[row.ClientID]; !found || filter != row.TopicFilter {
					return false, fmt.Sprintf("unexpected subscription %s/%s", row.ClientID, row.TopicFilter), nil
				}
				if _, duplicate := seen[row.ClientID]; duplicate {
					return false, fmt.Sprintf("duplicate subscription for client %s", row.ClientID), nil
				}
				seen[row.ClientID] = struct{}{}
			}
		}
		return true, "all subscriptions applied", nil
	})
}
