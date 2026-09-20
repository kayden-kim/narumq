package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
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
		subscribe.WithMetrics(metricsRegistry),
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
			Metrics:              metricsRegistry,
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
	managementServer := managementhttp.NewServer(
		managementhttp.ServerConfig{
			AllowAnonymousProbes:  true,
			AllowAnonymousMetrics: true,
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
	if err := json.Unmarshal(get("/api/v1/subscriptions"), &subscriptionsEnvelope); err != nil {
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
	if err := json.Unmarshal(get("/api/v1/retained"), &retainedEnvelope); err != nil {

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
