package integration

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"narumq/internal/adapters/clock"
	managementhttp "narumq/internal/adapters/http/management"
	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/adapters/memory/sessionstore"
	adaptersmetrics "narumq/internal/adapters/metrics"
	tcpadapter "narumq/internal/adapters/tcp"
	"narumq/internal/application/connect"
	appmanagement "narumq/internal/application/management"
	appobservability "narumq/internal/application/observability"
	observationports "narumq/internal/application/ports/observability"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
)

func TestManagementAPIObservesLiveMQTTState(t *testing.T) {
	mqttListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer mqttListener.Close()
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer httpListener.Close()

	metricsRegistry := adaptersmetrics.NewRegistry()
	routes := routing.NewTable()
	metricsRegistry.ObserveSubscriptions(routes)
	deliveries, err := deliveryqueue.New(64)
	if err != nil {
		t.Fatal(err)
	}
	retainedRepository, err := memoryretained.New(100, 1<<20, metricsRegistry)
	if err != nil {
		t.Fatal(err)
	}
	sessions := sessionstore.New()
	systemClock := clock.SystemClock{}
	willBridge := tcpadapter.NewWillBridge()

	connections, err := connect.New(
		systemClock,
		sessions,
		deliveries,
		routes,
		clock.SystemScheduler{},
		willBridge,
		connect.WithMetrics(metricsRegistry),
	)
	if err != nil {
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(
		routes,
		deliveries,
		retainedRepository,
	)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(
		systemClock,
		routes,
		deliveries,
		retainedRepository,
		publish.WithMetrics(metricsRegistry),
	)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	tcpServer, err := tcpadapter.NewServer(
		mqttListener,
		tcpadapter.Config{
			Clock:                systemClock,
			MaxPacketSize:        1 << 20,
			ConnectTimeout:       time.Second,
			WriteTimeout:         time.Second,
			ControlQueueCapacity: 16,
			ReceiveMaximum:       64, PacketReadTimeout: 10 * time.Second,
			Metrics: metricsRegistry,
		},
		decoder,
		encoder,
		connections,
		subscriptions,
		publications,
		deliveries,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	willBridge.Bind(tcpServer)

	managementState := appmanagement.NewService(
		appmanagement.ServiceConfig{
			ClusterID:      "integration-cluster",
			NodeID:         "integration-node",
			ReleaseVersion: "test",
		},
		sessions,
		routes,
		retainedRepository,
		metricsRegistry,
	)
	managementState.SetNodeState("READY", nil)
	observabilitySource := &managementAPIObservabilitySource{
		state: managementState, metrics: metricsRegistry, clock: systemClock, localNodeID: "integration-node",
	}
	observabilityService, err := appobservability.NewService(
		observabilitySource, observabilitySource, nil, systemClock,
	)
	if err != nil {
		t.Fatal(err)
	}
	managementServer := managementhttp.NewServer(
		managementhttp.ServerConfig{
			AllowAnonymousProbes:  true,
			AllowAnonymousMetrics: true,
			Observability:         observabilityService,
		},
		managementState,
		managementState,
		metricsRegistry,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpDone := make(chan error, 1)
	httpDone := make(chan error, 1)
	go func() { tcpDone <- tcpServer.Serve(ctx) }()
	go func() { httpDone <- managementServer.Serve(httpListener) }()
	defer func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		_ = managementServer.Shutdown(shutdownCtx)
		shutdownCancel()
		select {
		case err := <-tcpDone:
			if err != nil {
				t.Errorf("TCP server shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("TCP server did not stop")
		}
		select {
		case err := <-httpDone:
			if err != nil && err != http.ErrServerClosed {
				t.Errorf("HTTP server shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("HTTP server did not stop")
		}
	}()

	mqttConn, err := net.DialTimeout("tcp", mqttListener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer mqttConn.Close()
	writePacket(t, mqttConn, encoder, &mqtt.Connect{Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "management-client"})
	connAck := readPacket(t, mqttConn, decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
	if connAck.ReasonCode != 0 {
		t.Fatalf("CONNECT rejected: %#v", connAck)
	}

	writePacket(t, mqttConn, encoder, &mqtt.Subscribe{
		Version:  mqtt.ProtocolV5,
		PacketID: 1,
		Subscriptions: []mqtt.Subscription{{
			Filter: "management/topic",
			QoS:    mqtt.QoSAtMostOnce,
		}},
	})
	subAck := readPacket(t, mqttConn, decoder, mqtt.ProtocolV5).(*mqtt.SubAck)
	if len(subAck.ReasonCodes) != 1 || subAck.ReasonCodes[0] != 0 {
		t.Fatalf("SUBSCRIBE rejected: %#v", subAck)
	}
	writePacket(t, mqttConn, encoder, &mqtt.Publish{
		Version: mqtt.ProtocolV5,
		Topic:   "management/topic",
		Payload: []byte("payload"),
		Retain:  true,
		QoS:     mqtt.QoSAtMostOnce,
	})

	baseURL := "http://" + httpListener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	get := func(path string) []byte {
		t.Helper()
		response, err := client.Get(baseURL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, body = %s", path, response.StatusCode, body)
		}
		return body
	}

	metricsBody := string(get("/metrics"))
	if !strings.Contains(metricsBody, "narumq_connections_active 1") {
		t.Fatalf("metrics missing active connection: %s", metricsBody)
	}
	if !strings.Contains(metricsBody, `narumq_packets_received_total{packet_type="CONNECT"} 1`) {
		t.Fatalf("metrics missing CONNECT counter: %s", metricsBody)
	}

	var clusterEnvelope struct {
		Data struct {
			ClusterID string `json:"clusterId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(get("/api/v1/cluster"), &clusterEnvelope); err != nil {
		t.Fatal(err)
	}
	if clusterEnvelope.Data.ClusterID != "integration-cluster" {
		t.Fatalf("unexpected cluster response: %+v", clusterEnvelope)
	}

	var nodesEnvelope struct {
		Data []struct {
			NodeID string `json:"nodeId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(get("/api/v1/nodes"), &nodesEnvelope); err != nil {
		t.Fatal(err)
	}
	if len(nodesEnvelope.Data) != 1 || nodesEnvelope.Data[0].NodeID != "integration-node" {
		t.Fatalf("unexpected nodes response: %+v", nodesEnvelope)
	}

	var tenantsEnvelope struct {
		Data []struct {
			TenantID string `json:"tenantId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(get("/api/v1/tenants"), &tenantsEnvelope); err != nil {
		t.Fatal(err)
	}
	if len(tenantsEnvelope.Data) != 1 || tenantsEnvelope.Data[0].TenantID != "default" {
		t.Fatalf("unexpected tenants response: %+v", tenantsEnvelope)
	}

	var clientsEnvelope struct {
		Data []struct {
			ClientID string `json:"clientId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(get("/api/v1/clients"), &clientsEnvelope); err != nil {
		t.Fatal(err)
	}
	if len(clientsEnvelope.Data) != 1 || clientsEnvelope.Data[0].ClientID != "management-client" {
		t.Fatalf("unexpected clients response: %+v", clientsEnvelope)
	}

	var subscriptionsEnvelope struct {
		Data []struct {
			TopicFilter string `json:"topicFilter"`
		} `json:"data"`
	}
	if err := json.Unmarshal(get("/api/v1/subscriptions?tenantId=default"), &subscriptionsEnvelope); err != nil {
		t.Fatal(err)
	}
	if len(subscriptionsEnvelope.Data) != 1 || subscriptionsEnvelope.Data[0].TopicFilter != "management/topic" {
		t.Fatalf("unexpected subscriptions response: %+v", subscriptionsEnvelope)
	}

	var retainedEnvelope struct {
		Data []struct {
			Topic        string `json:"topic"`
			PayloadBytes int    `json:"payloadBytes"`
		} `json:"data"`
	}
	if err := json.Unmarshal(get("/api/v1/retained?tenantId=default"), &retainedEnvelope); err != nil {

		t.Fatal(err)
	}
	if len(retainedEnvelope.Data) != 1 || retainedEnvelope.Data[0].Topic != "management/topic" || retainedEnvelope.Data[0].PayloadBytes != len("payload") {
		t.Fatalf("unexpected retained response: %+v", retainedEnvelope)
	}
	openMetricsRequest, err := http.NewRequest(http.MethodGet, baseURL+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	openMetricsRequest.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	openMetricsResponse, err := client.Do(openMetricsRequest)
	if err != nil {
		t.Fatal(err)
	}
	openMetricsResponse.Body.Close()
	if openMetricsResponse.StatusCode != http.StatusOK || !strings.Contains(openMetricsResponse.Header.Get("Content-Type"), "application/openmetrics-text") {
		t.Fatalf("OpenMetrics response = %d, %q", openMetricsResponse.StatusCode, openMetricsResponse.Header.Get("Content-Type"))
	}

	for _, probe := range []struct {
		path string
		want int
	}{
		{path: "/health/startup", want: http.StatusOK},
		{path: "/health/drain", want: http.StatusServiceUnavailable},
	} {
		response, err := client.Get(baseURL + probe.path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != probe.want {
			t.Fatalf("%s status = %d, want %d", probe.path, response.StatusCode, probe.want)
		}
	}

	for _, requestCase := range []struct {
		method string
		path   string
		want   int
	}{
		{method: http.MethodGet, path: "/api/v1/nodes?state=invalid", want: http.StatusBadRequest},
		{method: http.MethodGet, path: "/api/v1/clients?cursor=bad", want: http.StatusBadRequest},
		{method: http.MethodGet, path: "/api/v1/clients/default/missing", want: http.StatusNotFound},
		{method: http.MethodGet, path: "/api/v1/clients?protocol=invalid", want: http.StatusBadRequest},
		{method: http.MethodPost, path: "/api/v1/cluster", want: http.StatusMethodNotAllowed},
	} {
		request, err := http.NewRequest(requestCase.method, baseURL+requestCase.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != requestCase.want {
			t.Fatalf("%s %s status = %d, want %d", requestCase.method, requestCase.path, response.StatusCode, requestCase.want)
		}
	}

	if response, err := client.Get(baseURL + "/health/ready"); err != nil {
		t.Fatal(err)
	} else {
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("readiness status = %d", response.StatusCode)
		}
	}
}

// Traceability: UI-03; docs/specs/management-api-and-observability.md §7.1.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-002, management-api-and-observability.md:MGT-SCN-029, management-api-and-observability.md:MGT-SCN-030
func TestMQTTRejectionsAndOversizeDropsAreObservable(t *testing.T) {
	mqttListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer mqttListener.Close()

	metricsRegistry := adaptersmetrics.NewRegistry()
	routes := routing.NewTable()
	metricsRegistry.ObserveSubscriptions(routes)
	deliveries, err := deliveryqueue.New(64)
	if err != nil {
		t.Fatal(err)
	}
	retainedRepository, err := memoryretained.New(100, 1<<20, metricsRegistry)
	if err != nil {
		t.Fatal(err)
	}
	sessions := sessionstore.New()
	systemClock := clock.SystemClock{}
	willBridge := tcpadapter.NewWillBridge()
	connections, err := connect.New(
		systemClock,
		sessions,
		deliveries,
		routes,
		clock.SystemScheduler{},
		willBridge,
		connect.WithMetrics(metricsRegistry),
	)
	if err != nil {
		t.Fatal(err)
	}
	subscriptions, err := subscribe.New(
		routes,
		deliveries,
		retainedRepository,
	)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(
		systemClock,
		routes,
		deliveries,
		retainedRepository,
		publish.WithMetrics(metricsRegistry),
	)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := mqtt.NewDecoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := mqtt.NewEncoder(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	tcpServer, err := tcpadapter.NewServer(
		mqttListener,
		tcpadapter.Config{
			Clock:                systemClock,
			MaxPacketSize:        1 << 20,
			ConnectTimeout:       time.Second,
			WriteTimeout:         time.Second,
			ControlQueueCapacity: 16,
			ReceiveMaximum:       64, PacketReadTimeout: 10 * time.Second,
			Metrics: metricsRegistry,
		},
		decoder,
		encoder,
		connections,
		subscriptions,
		publications,
		deliveries,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	willBridge.Bind(tcpServer)

	managementState := appmanagement.NewService(
		appmanagement.ServiceConfig{
			ClusterID:      "integration-cluster",
			NodeID:         "integration-node",
			ReleaseVersion: "test",
		},
		sessions,
		routes,
		retainedRepository,
		metricsRegistry,
	)
	managementState.SetNodeState("READY", nil)
	managementServer := httptest.NewServer(managementhttp.NewRouter(
		managementhttp.ServerConfig{
			AllowAnonymousMetrics: true,
		},
		managementState,
		managementState,
		metricsRegistry,
	))
	defer managementServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcpDone := make(chan error, 1)
	go func() { tcpDone <- tcpServer.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-tcpDone:
			if err != nil {
				t.Errorf("TCP server shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("TCP server did not stop")
		}
	}()

	rawConnect := func(protocolName string, level byte, clientID string) []byte {
		body := make([]byte, 0, 32+len(protocolName)+len(clientID))
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(protocolName)))
		body = append(body, length[:]...)
		body = append(body, protocolName...)
		body = append(body, level, 0x02, 0x00, 0x3c)
		binary.BigEndian.PutUint16(length[:], uint16(len(clientID)))
		body = append(body, length[:]...)
		body = append(body, clientID...)
		return append([]byte{0x10, byte(len(body))}, body...)
	}

	for _, testCase := range []struct {
		name         string
		protocolName string
		level        byte
		clientID     string
	}{
		{name: "MQIsdp level 3", protocolName: "MQIsdp", level: 3, clientID: "legacy-mqisdp"},
		{name: "MQTT level 3", protocolName: "MQTT", level: 3, clientID: "legacy-mqtt"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			connection, err := net.DialTimeout("tcp", mqttListener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			if _, err := connection.Write(rawConnect(testCase.protocolName, testCase.level, testCase.clientID)); err != nil {
				t.Fatal(err)
			}
			if testCase.protocolName == "MQIsdp" {
				expectClosed(t, connection)
				return
			}
			packet := readPacket(t, connection, decoder, mqtt.ProtocolV311)
			connAck, ok := packet.(*mqtt.ConnAck)
			if !ok || connAck.ReasonCode != 0x01 {
				t.Fatalf("MQTT level 3 response = %#v, want MQTT 3.1.1 CONNACK 0x01", packet)
			}
			expectClosed(t, connection)
		})
	}

	identifierRejected, err := net.DialTimeout("tcp", mqttListener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	writePacket(t, identifierRejected, encoder, &mqtt.Connect{
		Version: mqtt.ProtocolV311, CleanStart: true,
	})
	identifierAck, ok := readPacket(t, identifierRejected, decoder, mqtt.ProtocolV311).(*mqtt.ConnAck)
	if !ok || identifierAck.ReasonCode != 0x02 {
		t.Fatalf("empty MQTT 3.1.1 client ID CONNACK = %#v, want reason 0x02", identifierAck)
	}
	expectClosed(t, identifierRejected)
	_ = identifierRejected.Close()

	subscriber, err := net.DialTimeout("tcp", mqttListener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	writePacket(t, subscriber, encoder, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "oversize-subscriber", MaximumPacketSize: 64,
	})
	subscriberAck, ok := readPacket(t, subscriber, decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
	if !ok || subscriberAck.ReasonCode != 0 {
		t.Fatalf("oversize subscriber CONNACK = %#v", subscriberAck)
	}
	writePacket(t, subscriber, encoder, &mqtt.Subscribe{
		Version: mqtt.ProtocolV5, PacketID: 1,
		Subscriptions: []mqtt.Subscription{{Filter: "oversize/#", QoS: mqtt.QoSAtMostOnce}},
	})
	subscribeAck, ok := readPacket(t, subscriber, decoder, mqtt.ProtocolV5).(*mqtt.SubAck)
	if !ok || len(subscribeAck.ReasonCodes) != 1 || subscribeAck.ReasonCodes[0] != 0 {
		t.Fatalf("oversize subscriber SUBACK = %#v", subscribeAck)
	}

	publisher, err := net.DialTimeout("tcp", mqttListener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	writePacket(t, publisher, encoder, &mqtt.Connect{
		Version: mqtt.ProtocolV5, CleanStart: true, ClientID: "oversize-publisher",
	})
	publisherAck, ok := readPacket(t, publisher, decoder, mqtt.ProtocolV5).(*mqtt.ConnAck)
	if !ok || publisherAck.ReasonCode != 0 {
		t.Fatalf("oversize publisher CONNACK = %#v", publisherAck)
	}
	writePacket(t, publisher, encoder, &mqtt.Publish{
		Version: mqtt.ProtocolV5, Topic: "oversize/event", Payload: make([]byte, 128),
	})
	expectNoPacket(t, subscriber)
	writePacket(t, publisher, encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
	expectClosed(t, publisher)
	_ = publisher.Close()
	writePacket(t, subscriber, encoder, &mqtt.Disconnect{Version: mqtt.ProtocolV5})
	expectClosed(t, subscriber)
	_ = subscriber.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	metricsResponse, err := client.Get(managementServer.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metricsBody, err := io.ReadAll(metricsResponse.Body)
	metricsResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if metricsResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, body = %s", metricsResponse.StatusCode, metricsBody)
	}
	if want := `narumq_mqtt_connect_rejected_total{protocol="3.1.0",reason="unsupported_protocol_version"} 2`; !strings.Contains(string(metricsBody), want) {
		t.Fatalf("metrics missing MQTT 3.1.0 rejection count: %s", metricsBody)
	}
	if want := `narumq_mqtt_connect_rejected_total{protocol="MQTT_3_1_1",reason="identifier_rejected"} 1`; !strings.Contains(string(metricsBody), want) {
		t.Fatalf("metrics missing MQTT 3.1.1 identifier rejection: %s", metricsBody)
	}
	if want := `narumq_messages_dropped_total{reason="packet_too_large"} 1`; !strings.Contains(string(metricsBody), want) {
		t.Fatalf("metrics missing oversized packet drop: %s", metricsBody)
	}

	sessionsResponse, err := client.Get(managementServer.URL + "/api/v1/clients")
	if err != nil {
		t.Fatal(err)
	}
	var sessionsEnvelope struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(sessionsResponse.Body).Decode(&sessionsEnvelope); err != nil {
		sessionsResponse.Body.Close()
		t.Fatal(err)
	}
	sessionsResponse.Body.Close()
	if sessionsResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/v1/clients status = %d", sessionsResponse.StatusCode)
	}
	if len(sessionsEnvelope.Data) != 0 {
		t.Fatalf("unsupported MQTT 3.1.0 CONNECT created sessions: %d", len(sessionsEnvelope.Data))
	}
}

type managementAPIObservabilitySource struct {
	state       *appmanagement.Service
	metrics     *adaptersmetrics.Registry
	clock       clock.SystemClock
	localNodeID string
}

func (source *managementAPIObservabilitySource) ClusterSnapshot(ctx context.Context) (observationports.ClusterSnapshotDTO, error) {
	if err := ctx.Err(); err != nil {
		return observationports.ClusterSnapshotDTO{}, err
	}
	summary := source.state.GetClusterSummary(ctx)
	return observationports.ClusterSnapshotDTO{
		ClusterID: summary.ClusterID, ReleaseVersion: summary.ReleaseVersion, LocalNodeID: source.localNodeID,
		Members: []observationports.MemberDTO{{
			NodeID: source.localNodeID, Phase: observationports.MembershipPhaseStandalone,
		}},
		QuorumSize: summary.QuorumSize, ReplicationFactor: summary.ReplicationFactor,
		WriteQuorum: summary.WriteQuorum, LeaderNodeID: summary.LeaderNodeID,
		UnderReplicated: summary.UnderReplicated, SubscriptionCount: uint64(max(summary.SubscriptionCount, 0)),
	}, nil
}

func (source *managementAPIObservabilitySource) FreshQuorumAvailable(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return source.state.GetClusterSummary(ctx).QuorumAvailable, nil
}

func (source *managementAPIObservabilitySource) CaptureNodeSnapshot(ctx context.Context) (observationports.NodeSnapshotDTO, error) {
	if err := ctx.Err(); err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	node, err := source.state.GetNodeSummary(ctx, source.localNodeID)
	if err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	if node == nil {
		return observationports.NodeSnapshotDTO{}, errors.New("local node snapshot is unavailable")
	}
	state, ready := observationports.NodeState(node.State), node.Ready
	uptimeMillis := node.UptimeMillis
	connectionCount := uint64(max(node.ConnectionCount, 0))
	sessionCount := uint64(max(source.state.GetClusterSummary(ctx).SessionCount, 0))
	return observationports.NodeSnapshotDTO{
		ObservedAt: source.clock.Now().UTC(), Version: &node.Version,
		UptimeMillis: &uptimeMillis, State: &state, Ready: &ready,
		ReadinessReason: node.ReadinessReason,
		ConnectionCount: &connectionCount, SessionCount: &sessionCount,
	}, nil
}

func (source *managementAPIObservabilitySource) CaptureMetricsSnapshot(ctx context.Context) (observationports.MetricsSnapshotDTO, error) {
	if err := ctx.Err(); err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	snapshot, err := source.metrics.CaptureMetrics()
	if err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	return observationports.MetricsSnapshotDTO{
		ObservedAt: source.clock.Now().UTC(), StartedAt: source.metrics.StartedAt(), Snapshot: snapshot,
	}, nil
}
