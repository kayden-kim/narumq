package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	managementRequestTimeout = 15 * time.Second
	mqttReadTimeout          = 10 * time.Second
)

type apiError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

type apiEnvelope[T any] struct {
	Data  T         `json:"data"`
	Error *apiError `json:"error"`
}

type healthData struct {
	Status string  `json:"status"`
	Reason *string `json:"reason"`
}

type healthObservation struct {
	HTTPStatus   int    `json:"httpStatus"`
	Status       string `json:"status,omitempty"`
	Reason       string `json:"reason,omitempty"`
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	DurationMs   int64  `json:"durationMs"`
}

type clusterData struct {
	ClusterID       string  `json:"clusterId"`
	QuorumAvailable bool    `json:"quorumAvailable"`
	LeaderNodeID    *string `json:"leaderNodeId"`
}

type queueSample struct {
	OutboundMessages int    `json:"outboundMessages"`
	OutboundBytes    int    `json:"outboundBytes"`
	RejectedMessages uint64 `json:"rejectedMessages"`
}

type nodeData struct {
	NodeID            string       `json:"nodeId"`
	UptimeMillis      int64        `json:"uptimeMillis"`
	State             string       `json:"state"`
	Ready             bool         `json:"ready"`
	ReadinessReason   *string      `json:"readinessReason"`
	Role              string       `json:"role"`
	LastAppliedIndex  int64        `json:"lastAppliedIndex"`
	StoragePressure   bool         `json:"storagePressure"`
	ReplicationHealth string       `json:"replicationHealth"`
	LastErrorCode     string       `json:"lastErrorCode"`
	ReplicationQueue  *queueSample `json:"replicationQueue"`
}

type subscriptionData struct {
	TenantID    string  `json:"tenantId"`
	ClientID    string  `json:"clientId"`
	TopicFilter string  `json:"topicFilter"`
	QoS         int     `json:"qos"`
	SharedGroup *string `json:"sharedGroup"`
}

type subscriptionPage struct {
	HasMore bool `json:"hasMore"`
}

type subscriptionEnvelope struct {
	Data  []subscriptionData `json:"data"`
	Page  subscriptionPage   `json:"page"`
	Error *apiError          `json:"error"`
}

type apiHTTPClient struct {
	client     *http.Client
	adminToken string
	management map[string]string
}

func newAPIHTTPClient(adminToken string, managementPorts map[string]string) *apiHTTPClient {
	return &apiHTTPClient{
		client:     &http.Client{Timeout: managementRequestTimeout},
		adminToken: adminToken,
		management: managementPorts,
	}
}

func (client *apiHTTPClient) get(ctx context.Context, nodeID, path string, destination any) (int, int64, error) {
	port, ok := client.management[nodeID]
	if !ok {
		return 0, 0, fmt.Errorf("management port for node %s is unavailable", nodeID)
	}
	endpoint := "http://127.0.0.1:" + port + path
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("create management request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+client.adminToken)
	request.Header.Set("Accept", "application/json")
	startedAt := time.Now()
	response, err := client.client.Do(request)
	duration := time.Since(startedAt).Milliseconds()
	if err != nil {
		return 0, duration, fmt.Errorf("management GET %s: %w", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return response.StatusCode, duration, fmt.Errorf("read management response: %w", err)
	}
	if err := json.Unmarshal(body, destination); err != nil {
		return response.StatusCode, duration, fmt.Errorf("decode management response: %w", err)
	}
	return response.StatusCode, duration, nil
}

func (client *apiHTTPClient) health(ctx context.Context, nodeID, path string) (healthObservation, error) {
	startedAt := time.Now()
	var response apiEnvelope[healthData]
	status, duration, err := client.get(ctx, nodeID, path, &response)
	observation := healthObservation{HTTPStatus: status, DurationMs: duration}
	if err != nil {
		observation.DurationMs = time.Since(startedAt).Milliseconds()
		return observation, err
	}
	if response.Error != nil {
		observation.ErrorCode = response.Error.Code
		observation.ErrorMessage = response.Error.Message
		if status == http.StatusServiceUnavailable {
			if state, ok := response.Error.Details["status"].(string); ok {
				observation.Status = state
			}
		}
		return observation, nil
	}
	observation.Status = response.Data.Status
	if response.Data.Reason != nil {
		observation.Reason = *response.Data.Reason
	}
	return observation, nil
}

func (client *apiHTTPClient) cluster(ctx context.Context, nodeID string) (clusterData, int, int64, error) {
	var response apiEnvelope[clusterData]
	status, duration, err := client.get(ctx, nodeID, "/api/v1/cluster", &response)
	if err != nil {
		return clusterData{}, status, duration, err
	}
	if response.Error != nil {
		return clusterData{}, status, duration, fmt.Errorf("cluster API %s: %s", response.Error.Code, response.Error.Message)
	}
	return response.Data, status, duration, nil
}

func (client *apiHTTPClient) node(ctx context.Context, observerID, nodeID string) (nodeData, int, int64, error) {
	var response apiEnvelope[nodeData]
	path := "/api/v1/nodes/" + url.PathEscape(nodeID)
	status, duration, err := client.get(ctx, observerID, path, &response)
	if err != nil {
		return nodeData{}, status, duration, err
	}
	if response.Error != nil {
		return nodeData{}, status, duration, fmt.Errorf("node API %s: %s", response.Error.Code, response.Error.Message)
	}
	return response.Data, status, duration, nil
}

func (client *apiHTTPClient) subscriptions(ctx context.Context, nodeID string) (subscriptionEnvelope, int, int64, error) {
	var response subscriptionEnvelope
	path := "/api/v1/subscriptions?" + url.Values{"tenantId": []string{"chaos"}, "limit": []string{"50"}}.Encode()
	status, duration, err := client.get(ctx, nodeID, path, &response)
	if err != nil {
		return subscriptionEnvelope{}, status, duration, err
	}
	if response.Error != nil {
		return subscriptionEnvelope{}, status, duration, fmt.Errorf("subscriptions API %s: %s", response.Error.Code, response.Error.Message)
	}
	return response, status, duration, nil
}

func validateQueueSample(sample queueSample) error {
	if sample.OutboundMessages < 0 || sample.OutboundBytes < 0 {
		return errors.New("outbound queue values must be non-negative")
	}
	if sample.OutboundMessages > maxOutboundMessages {
		return fmt.Errorf("outbound message queue exceeded %d: %d", maxOutboundMessages, sample.OutboundMessages)
	}
	if sample.OutboundBytes > maxOutboundBytes {
		return fmt.Errorf("outbound byte queue exceeded %d: %d", maxOutboundBytes, sample.OutboundBytes)
	}
	return nil
}

func queueCapReached(sample queueSample) bool {
	return sample.OutboundMessages == maxOutboundMessages || sample.RejectedMessages > 0
}

type mqttProbeResult struct {
	Classification string `json:"classification"`
	ConnackCode    *byte  `json:"connackCode,omitempty"`
	SubackCode     *byte  `json:"subackCode,omitempty"`
	DurationMs     int64  `json:"durationMs"`
	Error          string `json:"error,omitempty"`
}

func probeMQTT(ctx context.Context, address, accessToken, clientID, topicFilter string) mqttProbeResult {
	startedAt := time.Now()
	connection, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return mqttProbeResult{Classification: "closed", DurationMs: time.Since(startedAt).Milliseconds(), Error: err.Error()}
	}
	defer connection.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stopClose()
	result := mqttProbeConnection(ctx, connection, accessToken, clientID, topicFilter)
	result.DurationMs = time.Since(startedAt).Milliseconds()
	return result
}

func mqttProbeConnection(ctx context.Context, connection net.Conn, accessToken, clientID, topicFilter string) mqttProbeResult {
	connectPacket, err := mqttConnectPacket(clientID, accessToken)
	if err != nil {
		return mqttProbeResult{Classification: "rejected", Error: err.Error()}
	}
	if err := writeMQTTPacket(connection, ctx, connectPacket); err != nil {
		return mqttProbeResult{Classification: "closed", Error: err.Error()}
	}
	connackHeader, connackBody, err := readMQTTPacket(connection, ctx)
	if err != nil {
		return mqttProbeResult{Classification: "closed", Error: err.Error()}
	}
	if connackHeader != 0x20 || len(connackBody) != 2 {
		return mqttProbeResult{Classification: "rejected", Error: "invalid CONNACK packet"}
	}
	connackCode := connackBody[1]
	result := mqttProbeResult{ConnackCode: &connackCode}
	if connackCode != 0 {
		result.Classification = "rejected"
		return result
	}

	subscribePacket, err := mqttSubscribePacket(topicFilter)
	if err != nil {
		return mqttProbeResult{Classification: "rejected", ConnackCode: &connackCode, Error: err.Error()}
	}
	if err := writeMQTTPacket(connection, ctx, subscribePacket); err != nil {
		return mqttProbeResult{Classification: "closed", ConnackCode: &connackCode, Error: err.Error()}
	}
	subackHeader, subackBody, err := readMQTTPacket(connection, ctx)
	if err != nil {
		return mqttProbeResult{Classification: "closed", ConnackCode: &connackCode, Error: err.Error()}
	}
	if subackHeader != 0x90 || len(subackBody) != 3 || binary.BigEndian.Uint16(subackBody[:2]) != 1 {
		return mqttProbeResult{Classification: "rejected", ConnackCode: &connackCode, Error: "invalid SUBACK packet"}
	}
	subackCode := subackBody[2]
	result.SubackCode = &subackCode
	if subackCode != 0 {
		result.Classification = "rejected"
		return result
	}
	result.Classification = "accepted"
	return result
}

func mqttConnectPacket(clientID, accessToken string) ([]byte, error) {
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 4, 0xc0, 0, 60}
	for _, field := range []string{clientID, "jwt", accessToken} {
		if len(field) > 65_535 {
			return nil, errors.New("MQTT CONNECT field exceeds the protocol length limit")
		}
		body = binary.BigEndian.AppendUint16(body, uint16(len(field)))
		body = append(body, field...)
	}
	return mqttPacket(0x10, body), nil
}

func mqttSubscribePacket(topicFilter string) ([]byte, error) {
	if len(topicFilter) > 65_535 {
		return nil, errors.New("MQTT topic filter exceeds the protocol length limit")
	}
	body := []byte{0, 1}
	body = binary.BigEndian.AppendUint16(body, uint16(len(topicFilter)))
	body = append(body, topicFilter...)
	body = append(body, 0)
	return mqttPacket(0x82, body), nil
}

func mqttPacket(header byte, body []byte) []byte {
	packet := []byte{header}
	remaining := len(body)
	for {
		digit := byte(remaining % 128)
		remaining /= 128
		if remaining > 0 {
			digit |= 0x80
		}
		packet = append(packet, digit)
		if remaining == 0 {
			break
		}
	}
	return append(packet, body...)
}

func writeMQTTPacket(connection net.Conn, ctx context.Context, packet []byte) error {
	if err := setMQTTDeadline(connection, ctx); err != nil {
		return err
	}
	if _, err := connection.Write(packet); err != nil {
		return fmt.Errorf("write MQTT packet: %w", err)
	}
	return nil
}

func readMQTTPacket(connection net.Conn, ctx context.Context) (byte, []byte, error) {
	if err := setMQTTDeadline(connection, ctx); err != nil {
		return 0, nil, err
	}
	var header [1]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return 0, nil, fmt.Errorf("read MQTT packet header: %w", err)
	}
	remaining := 0
	multiplier := 1
	for count := 0; count < 4; count++ {
		var encoded [1]byte
		if _, err := io.ReadFull(connection, encoded[:]); err != nil {
			return 0, nil, fmt.Errorf("read MQTT remaining length: %w", err)
		}
		remaining += int(encoded[0]&0x7f) * multiplier
		if encoded[0]&0x80 == 0 {
			if remaining > 1<<20 {
				return 0, nil, errors.New("MQTT packet body exceeds the probe limit")
			}
			body := make([]byte, remaining)
			if _, err := io.ReadFull(connection, body); err != nil {
				return 0, nil, fmt.Errorf("read MQTT packet body: %w", err)
			}
			return header[0], body, nil
		}
		multiplier *= 128
	}
	return 0, nil, errors.New("invalid MQTT remaining length encoding")
}

func setMQTTDeadline(connection net.Conn, ctx context.Context) error {
	deadline := time.Now().Add(mqttReadTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set MQTT deadline: %w", err)
	}
	return nil
}

func parseManagementPort(output []byte) (string, error) {
	value := strings.TrimSpace(string(output))
	const hostPrefix = "127.0.0.1:"
	if !strings.HasPrefix(value, hostPrefix) {
		return "", fmt.Errorf("invalid Docker host-port mapping %q", value)
	}
	port := strings.TrimPrefix(value, hostPrefix)
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65_535 {
		return "", fmt.Errorf("invalid Docker host-port mapping %q", value)
	}
	return port, nil
}
