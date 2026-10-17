package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ports "narumq/internal/application/ports/management"
)

type nodeConfigurationUseCaseStub struct {
	configuration ports.NodeConfigurationDTO
	err           error
	calls         int
}

func (stub *nodeConfigurationUseCaseStub) GetNodeConfiguration(context.Context) (ports.NodeConfigurationDTO, error) {
	stub.calls++
	return stub.configuration, stub.err
}

func nodeConfigurationTestRouter(service ports.NodeConfigurationUseCase, roles ...Role) http.Handler {
	return NewRouterWithMembership(
		ServerConfig{LocalNodeID: 1, APIKeys: []APIKeyCredential{{ID: "config", Key: "config-secret", Roles: roles}}},
		&mockStateProvider{}, nil, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, service,
	)
}

func TestNodeConfigurationRouteScopeRequestShapeAndPermission(t *testing.T) {
	service := &nodeConfigurationUseCaseStub{configuration: ports.NodeConfigurationDTO{
		Node: ports.NodeIdentityConfigurationDTO{
			ClusterID: "cluster-a", NodeID: 1, Role: "bootstrap", ListenAddress: "127.0.0.1:19001",
			AdvertisedAddress: "node1.example:19001", LocalDNSName: "node1.example",
		},
		Build: ports.NodeBuildConfigurationDTO{ReleaseVersion: "1.0.0", Features: []string{"cluster-policy"}},
	}}
	handler := nodeConfigurationTestRouter(service, RoleClusterAdmin)

	for _, testCase := range []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantBody   string
	}{
		{name: "local config", method: http.MethodGet, path: "/api/v1/nodes/1/config", wantStatus: http.StatusOK, wantBody: `"nodeId":"1"`},
		{name: "non-local node", method: http.MethodGet, path: "/api/v1/nodes/2/config", wantStatus: http.StatusConflict, wantBody: `"code":"node_not_local"`},
		{name: "non-canonical node ID", method: http.MethodGet, path: "/api/v1/nodes/01/config", wantStatus: http.StatusNotFound, wantBody: `"code":"resource_not_found"`},
		{name: "query not allowed", method: http.MethodGet, path: "/api/v1/nodes/1/config?unexpected=true", wantStatus: http.StatusBadRequest, wantBody: `"code":"invalid_request"`},
		{name: "body not allowed", method: http.MethodGet, path: "/api/v1/nodes/1/config", body: `{}`, wantStatus: http.StatusBadRequest, wantBody: `"code":"invalid_request"`},
		{name: "method not allowed", method: http.MethodPost, path: "/api/v1/nodes/1/config", wantStatus: http.StatusMethodNotAllowed},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(testCase.method, testCase.path, strings.NewReader(testCase.body))
			request.Header.Set("X-API-Key", "config-secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != testCase.wantStatus {
				t.Fatalf("status=%d body=%s, want %d", response.Code, response.Body.String(), testCase.wantStatus)
			}
			if testCase.wantBody != "" && !strings.Contains(response.Body.String(), testCase.wantBody) {
				t.Fatalf("body=%s, want %q", response.Body.String(), testCase.wantBody)
			}
			if testCase.wantStatus == http.StatusMethodNotAllowed && response.Header().Get("Allow") != http.MethodGet {
				t.Fatalf("Allow=%q, want GET", response.Header().Get("Allow"))
			}
		})
	}
	if service.calls != 1 {
		t.Fatalf("configuration use case calls=%d, want only the local request", service.calls)
	}

	readOnlyHandler := nodeConfigurationTestRouter(service, RoleReadOnly)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/config", nil)
	request.Header.Set("X-API-Key", "config-secret")
	response := httptest.NewRecorder()
	readOnlyHandler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || service.calls != 1 {
		t.Fatalf("ReadOnly status=%d calls=%d, want forbidden without use case call", response.Code, service.calls)
	}
}

func TestNodeConfigurationHandlerHidesServiceErrors(t *testing.T) {
	service := &nodeConfigurationUseCaseStub{err: errors.New("private configuration path")}
	handler := nodeConfigurationTestRouter(service, RoleClusterAdmin)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/config", nil)
	request.Header.Set("X-API-Key", "config-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), `"code":"configuration_unavailable"`) || strings.Contains(response.Body.String(), "private configuration path") {
		t.Fatalf("response=%d %s", response.Code, response.Body.String())
	}
}

// Traceability: CFG-SCN-46, PRD MQTT-10; docs/specs/configuration-and-runtime-options.md §6; docs/contracts/management-api-v1.md §1, §4.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-46
func TestNodeConfigurationResponseIncludesWebSocketSettings(t *testing.T) {
	type webSocketResponse struct {
		ListenAddress     string `json:"listenAddress"`
		TLSListenAddress  string `json:"tlsListenAddress"`
		MaxConnections    int    `json:"maxConnections"`
		ConnectionRate    int    `json:"connectionRate"`
		TLSMaxConnections int    `json:"tlsMaxConnections"`
		TLSConnectionRate int    `json:"tlsConnectionRate"`
	}
	for _, test := range []struct {
		name   string
		config *ports.MQTTWebSocketConfigurationDTO
		want   *webSocketResponse
	}{
		{name: "not configured"},
		{
			name: "configured",
			config: &ports.MQTTWebSocketConfigurationDTO{
				ListenAddress: "127.0.0.1:1884", TLSListenAddress: "0.0.0.0:8884",
				MaxConnections: 10, ConnectionRate: 20, TLSMaxConnections: 30, TLSConnectionRate: 40,
			},
			want: &webSocketResponse{
				ListenAddress: "127.0.0.1:1884", TLSListenAddress: "0.0.0.0:8884",
				MaxConnections: 10, ConnectionRate: 20, TLSMaxConnections: 30, TLSConnectionRate: 40,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mqttConfiguration := ports.MQTTListenerConfigurationDTO{WebSocket: test.config}
			if test.config != nil {
				mqttConfiguration.TLS = &ports.MQTTTLSConfigurationDTO{
					ClientAuth: "none", MinVersion: "1.2", CertificateConfigured: true, PrivateKeyConfigured: true,
				}
			}
			service := &nodeConfigurationUseCaseStub{configuration: ports.NodeConfigurationDTO{
				MQTT: mqttConfiguration,
			}}
			handler := nodeConfigurationTestRouter(service, RoleClusterAdmin)
			request := httptest.NewRequest(http.MethodGet, "/api/v1/nodes/1/config", nil)
			request.Header.Set("X-API-Key", "config-secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s, want %d", response.Code, response.Body.String(), http.StatusOK)
			}

			var envelope struct {
				Data struct {
					MQTT json.RawMessage `json:"mqtt"`
				} `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode response envelope: %v", err)
			}
			var mqttFields map[string]json.RawMessage
			if err := json.Unmarshal(envelope.Data.MQTT, &mqttFields); err != nil {
				t.Fatalf("decode MQTT configuration: %v", err)
			}
			webSocketJSON, exists := mqttFields["webSocket"]
			if !exists {
				t.Fatal("mqtt.webSocket is missing from response")
			}
			if test.want == nil {
				if string(webSocketJSON) != "null" {
					t.Fatalf("mqtt.webSocket = %s, want null", webSocketJSON)
				}
				return
			}
			var got webSocketResponse
			if err := json.Unmarshal(webSocketJSON, &got); err != nil {
				t.Fatalf("decode mqtt.webSocket: %v", err)
			}
			if got != *test.want {
				t.Fatalf("mqtt.webSocket = %+v, want %+v", got, *test.want)
			}
		})
	}
}
