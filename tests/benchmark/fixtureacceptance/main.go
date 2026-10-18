package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"narumq/internal/domain/mqtt"
	"narumq/tests/benchmark/internal/mqttclient"
)

func main() {
	address := flag.String("address", "127.0.0.1:11883", "")
	passwordFile := flag.String("password-file", "", "")
	flag.Parse()
	if *passwordFile == "" {
		fail("password file is required")
	}
	password, err := os.ReadFile(*passwordFile)
	if err != nil {
		fail("could not read fixture credential")
	}
	password = []byte(strings.TrimSpace(string(password)))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connect := func(clean bool) (*mqttclient.Client, mqttclient.ConnectTiming) {
		client, timing, err := mqttclient.Connect(ctx, mqttclient.ConnectOptions{
			Address: *address, ClientID: "acceptance-device", ProtocolVersion: mqtt.ProtocolV5,
			KeepAlive: 30, HandshakeTimeout: 10 * time.Second, CleanStart: clean,
			SessionExpiryInterval: 86400, Username: "acceptance-device", Password: password,
		})
		if err != nil {
			fail("fixture MQTT connection failed: %v", err)
		}
		return client, timing
	}
	client, first := connect(true)
	if err := client.Write(&mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 1, Subscriptions: []mqtt.Subscription{{Filter: "acceptance/offline/#", QoS: mqtt.QoSAtLeastOnce, NoLocal: true}}}, time.Second); err != nil {
		fail("fixture subscription failed: %v", err)
	}
	packet, err := client.Read(10 * time.Second)
	if err != nil {
		fail("fixture subscription acknowledgement failed: %v", err)
	}
	ack, ok := packet.(*mqtt.SubAck)
	if !ok || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] > 2 {
		fail("fixture subscription was rejected")
	}
	if err := client.Disconnect(time.Second); err != nil {
		fail("fixture MQTT disconnect failed: %v", err)
	}
	_ = client.Close()
	resumed, second := connect(false)
	if !second.SessionPresent {
		fail("fixture persistent MQTT session was not resumed")
	}
	if err := resumed.Write(&mqtt.Publish{Version: mqtt.ProtocolV5, QoS: mqtt.QoSAtLeastOnce, PacketID: 2, Retain: true, Topic: "acceptance/retained/fixture", Payload: []byte("synthetic-acceptance-payload-20261014")}, time.Second); err != nil {
		fail("fixture retained publish failed: %v", err)
	}
	for {
		packet, err = resumed.Read(10 * time.Second)
		if err != nil {
			fail("fixture retained publish acknowledgement failed: %v", err)
		}
		switch p := packet.(type) {
		case *mqtt.PubAck:
			if p.PacketID != 2 || p.ReasonCode >= 0x80 {
				fail("fixture retained publish was rejected")
			}
			goto published
		case *mqtt.Publish:
			if err := resumed.Write(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: p.PacketID}, time.Second); err != nil {
				fail("fixture delivery acknowledgement failed: %v", err)
			}
		default:
		}
	}
published:
	if err := resumed.Write(&mqtt.Subscribe{Version: mqtt.ProtocolV5, PacketID: 3, Subscriptions: []mqtt.Subscription{{Filter: "acceptance/retained/fixture", QoS: mqtt.QoSAtLeastOnce}}}, time.Second); err != nil {
		fail("retained-message verification subscription failed: %v", err)
	}
	packet, err = resumed.Read(10 * time.Second)
	if err != nil {
		fail("retained-message verification acknowledgement failed: %v", err)
	}
	ack, ok = packet.(*mqtt.SubAck)
	if !ok || len(ack.ReasonCodes) != 1 || ack.ReasonCodes[0] > 2 {
		fail("retained-message verification subscription was rejected")
	}
	packet, err = resumed.Read(10 * time.Second)
	if err != nil {
		fail("retained-message delivery failed: %v", err)
	}
	retained, ok := packet.(*mqtt.Publish)
	if !ok || retained.Topic != "acceptance/retained/fixture" || !retained.Retain || string(retained.Payload) != "synthetic-acceptance-payload-20261014" {
		fail("retained synthetic message did not match fixture expectations")
	}
	if err := resumed.Disconnect(time.Second); err != nil && !errors.Is(err, os.ErrClosed) {
		fail("fixture MQTT disconnect failed: %v", err)
	}
	_ = resumed.Close()
	fmt.Printf("PASS persistent_session_resumed=true retained_synthetic_message=true session_expiry_seconds=86400 first_connect_ms=%d resumed_connect_ms=%d\n", first.Latency.Milliseconds(), second.Latency.Milliseconds())
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
