package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/mqttclient"
)

const (
	username = "acceptance-device"
	clientID = "acceptance-device"
)

type marker struct {
	topic   string
	payload []byte
}

type result struct {
	Stage            string `json:"stage"`
	ObservedAt       string `json:"observedAt"`
	Topic            string `json:"topic"`
	QoS              byte   `json:"qos"`
	Retained         bool   `json:"retained"`
	PayloadSHA256    string `json:"payloadSha256"`
	SubAckReasonCode byte   `json:"subAckReasonCode"`
	SessionPresent   bool   `json:"sessionPresent"`
}

func main() {
	address := flag.String("address", "127.0.0.1:31883", "fixture-only localhost port-forward")
	passwordPath := flag.String("password-file", "", "fixture password file; contents are never printed")
	flag.Parse()
	if *passwordPath == "" {
		fatal(errors.New("password-file is required"))
	}
	passwordBytes, err := os.ReadFile(*passwordPath)
	if err != nil {
		fatal(err)
	}
	password := strings.TrimSpace(string(passwordBytes))
	if password == "" {
		fatal(errors.New("fixture password file is empty"))
	}
	markers := []marker{
		{topic: "acceptance/storage-oracle-20261014/qos1", payload: []byte("narumq-fixture-durable-ack-qos1-20261014")},
		{topic: "acceptance/storage-oracle-20261014/qos2", payload: []byte("narumq-fixture-durable-ack-qos2-20261014")},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, timing, err := mqttclient.Connect(ctx, mqttclient.ConnectOptions{
		Address: *address, ClientID: clientID, ProtocolVersion: mqtt.ProtocolV5,
		KeepAlive: 30, HandshakeTimeout: 5 * time.Second, CleanStart: true,
		Username: username, Password: []byte(password),
	})
	if err != nil {
		fatal(fmt.Errorf("fixture MQTT connect: %w", err))
	}
	defer client.Close()
	if err := client.Write(&mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 51,
		Subscriptions: []mqtt.Subscription{{Filter: "acceptance/storage-oracle-20261014/#", QoS: mqtt.QoSExactlyOnce}},
	}, 5*time.Second); err != nil {
		fatal(fmt.Errorf("send SUBSCRIBE: %w", err))
	}
	packet, err := client.Read(10 * time.Second)
	if err != nil {
		fatal(fmt.Errorf("read SUBACK: %w", err))
	}
	suback, ok := packet.(*mqtt.SubAck)
	if !ok || suback.PacketID != 51 || len(suback.ReasonCodes) != 1 || suback.ReasonCodes[0] != byte(mqtt.QoSExactlyOnce) {
		fatal(fmt.Errorf("unexpected SUBACK: %#v", packet))
	}
	seen := make(map[string]bool, len(markers))
	for range markers {
		packet, err = client.Read(10 * time.Second)
		if err != nil {
			fatal(fmt.Errorf("read retained PUBLISH: %w", err))
		}
		got, ok := packet.(*mqtt.Publish)
		if !ok || !got.Retain || got.QoS == mqtt.QoSAtMostOnce {
			fatal(fmt.Errorf("retained payload verification failed (packet type %T)", packet))
		}
		var expected *marker
		for i := range markers {
			if markers[i].topic == got.Topic {
				expected = &markers[i]
			}
		}
		if expected == nil || !equal(got.Payload, expected.payload) || seen[got.Topic] {
			fatal(fmt.Errorf("unexpected or duplicate retained payload for topic %q", got.Topic))
		}
		seen[got.Topic] = true
		if got.QoS == mqtt.QoSAtLeastOnce {
			if err := client.Write(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: got.PacketID}, 5*time.Second); err != nil {
				fatal(fmt.Errorf("send PUBACK: %w", err))
			}
		} else {
			if err := client.Write(&mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: got.PacketID}, 5*time.Second); err != nil {
				fatal(fmt.Errorf("send PUBREC: %w", err))
			}
			release, err := client.Read(10 * time.Second)
			if err != nil {
				fatal(fmt.Errorf("read retained PUBREL: %w", err))
			}
			pubrel, ok := release.(*mqtt.PubRel)
			if !ok || pubrel.PacketID != got.PacketID {
				fatal(fmt.Errorf("unexpected retained PUBREL: %#v", release))
			}
			if err := client.Write(&mqtt.PubComp{Version: mqtt.ProtocolV5, PacketID: got.PacketID}, 5*time.Second); err != nil {
				fatal(fmt.Errorf("send PUBCOMP: %w", err))
			}
		}
		digest := sha256.Sum256(got.Payload)
		if err := json.NewEncoder(os.Stdout).Encode(result{
			Stage: "retained_payload_verified", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Topic: got.Topic, QoS: byte(got.QoS), Retained: got.Retain,
			PayloadSHA256: hex.EncodeToString(digest[:]), SubAckReasonCode: suback.ReasonCodes[0],
			SessionPresent: timing.SessionPresent,
		}); err != nil {
			fatal(err)
		}
	}
	if len(seen) != len(markers) {
		fatal(errors.New("not all expected retained payloads were observed"))
	}
	_ = client.Disconnect(2 * time.Second)
}

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fatal(err error) {
	_ = json.NewEncoder(os.Stderr).Encode(map[string]string{"error": err.Error()})
	os.Exit(1)
}
