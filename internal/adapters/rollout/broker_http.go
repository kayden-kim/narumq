package rollout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	rolloutapp "narumq/internal/application/rollout"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	managementAPIBase = "/api/v1"
	maxManagementBody = 1 << 20
)

type ManagementClient struct {
	client *http.Client
}

func NewManagementClient() *ManagementClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &ManagementClient{client: &http.Client{
		Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (client *ManagementClient) ClusterFeatures(ctx context.Context, namespace, endpoint, token string) (rolloutapp.ClusterFeaturesDTO, error) {
	var data struct {
		ActiveFeatures []string `json:"activeFeatures"`
		Members        []struct {
			NodeID         string   `json:"nodeId"`
			ReleaseVersion string   `json:"releaseVersion"`
			Features       []string `json:"features"`
		} `json:"members"`
	}
	if err := client.request(ctx, namespace, endpoint, token, http.MethodGet, managementAPIBase+"/cluster/features", "", nil, &data); err != nil {
		return rolloutapp.ClusterFeaturesDTO{}, err
	}
	features := rolloutapp.ClusterFeaturesDTO{ActiveFeatures: append([]string(nil), data.ActiveFeatures...), Members: make([]rolloutapp.MemberProfileDTO, 0, len(data.Members))}
	for _, member := range data.Members {
		id, err := parseNodeID(member.NodeID)
		if err != nil || member.ReleaseVersion == "" || member.Features == nil {
			return rolloutapp.ClusterFeaturesDTO{}, errors.New("cluster feature response contains incomplete member identity")
		}
		features.Members = append(features.Members, rolloutapp.MemberProfileDTO{NodeID: id, ReleaseVersion: member.ReleaseVersion, Features: append([]string(nil), member.Features...)})
	}
	if data.ActiveFeatures == nil {
		return rolloutapp.ClusterFeaturesDTO{}, errors.New("cluster feature response omitted activeFeatures")
	}
	return features, nil
}

func (client *ManagementClient) GetDrain(ctx context.Context, namespace, endpoint, token string, nodeID uint32, idempotencyKey string) (rolloutapp.DrainObservationDTO, bool, error) {
	var data drainResponse
	path := managementAPIBase + "/nodes/" + strconv.FormatUint(uint64(nodeID), 10) + "/drain"
	err := client.request(ctx, namespace, endpoint, token, http.MethodGet, path, "", nil, &data)
	if err != nil {
		var statusErr *httpStatusError
		if errors.As(err, &statusErr) && statusErr.status == http.StatusNotFound {
			return rolloutapp.DrainObservationDTO{}, false, nil
		}
		return rolloutapp.DrainObservationDTO{}, false, err
	}
	observation, err := data.dto()
	if err != nil || observation.NodeID != nodeID {
		return rolloutapp.DrainObservationDTO{}, false, errors.New("drain GET returned incomplete or mismatched operation evidence")
	}
	return observation, true, nil
}

func (client *ManagementClient) StartDrain(ctx context.Context, namespace, endpoint, token string, nodeID uint32, idempotencyKey string) (rolloutapp.DrainObservationDTO, error) {
	var data drainResponse
	path := managementAPIBase + "/nodes/" + strconv.FormatUint(uint64(nodeID), 10) + "/drain"
	if err := client.request(ctx, namespace, endpoint, token, http.MethodPost, path, idempotencyKey, []byte("{}"), &data); err != nil {
		return rolloutapp.DrainObservationDTO{}, err
	}
	observation, err := data.dto()
	if err != nil || observation.NodeID != nodeID {
		return rolloutapp.DrainObservationDTO{}, errors.New("drain POST returned incomplete or mismatched operation evidence")
	}
	return observation, nil
}

func (client *ManagementClient) CancelDrain(ctx context.Context, namespace, endpoint, token string, nodeID uint32, idempotencyKey string) (rolloutapp.DrainObservationDTO, error) {
	var data drainResponse
	path := managementAPIBase + "/nodes/" + strconv.FormatUint(uint64(nodeID), 10) + "/drain"
	if err := client.request(ctx, namespace, endpoint, token, http.MethodDelete, path, idempotencyKey, nil, &data); err != nil {
		return rolloutapp.DrainObservationDTO{}, err
	}
	observation, err := data.dto()
	if err != nil || observation.NodeID != nodeID {
		return rolloutapp.DrainObservationDTO{}, errors.New("drain DELETE returned incomplete or mismatched operation evidence")
	}
	return observation, nil
}

func (client *ManagementClient) request(ctx context.Context, namespace, endpoint, token, method, path, idempotencyKey string, body []byte, target any) error {
	base, err := managementURL(endpoint, namespace)
	if err != nil || !validAPIKey(token) {
		return errors.New("management endpoint or credential is invalid")
	}
	base.Path = strings.TrimSuffix(base.Path, "/")
	base.Path += path
	var requestBody io.Reader
	if body != nil {
		requestBody = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, base.String(), requestBody)
	if err != nil {
		return errors.New("management request could not be constructed")
	}
	request.Header.Set("X-API-Key", token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.client.Do(request)
	if err != nil {
		return errors.New("management API request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxManagementBody))
		return &httpStatusError{status: response.StatusCode}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxManagementBody))
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := decoder.Decode(&envelope); err != nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return errors.New("management API response is not a data envelope")
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		return errors.New("management API response data is malformed")
	}
	return nil
}

type httpStatusError struct{ status int }

func (err *httpStatusError) Error() string {
	return fmt.Sprintf("management API returned HTTP %d", err.status)
}

type drainResponse struct {
	OperationID string            `json:"operationId"`
	NodeID      string            `json:"nodeId"`
	State       string            `json:"state"`
	Phase       string            `json:"phase"`
	StartedAt   string            `json:"startedAt"`
	DeadlineAt  string            `json:"deadlineAt"`
	Connections *drainConnections `json:"connections"`
	Sessions    *drainSessions    `json:"sessions"`
	Replication *drainReplication `json:"replication"`
	SafeToStop  *bool             `json:"safeToStop"`
	Failure     *drainFailure     `json:"failure"`
}

type drainConnections struct {
	Initial   *uint64 `json:"initial"`
	Remaining *uint64 `json:"remaining"`
}

type drainSessions struct {
	Ready     *uint64 `json:"stateReadyForTakeover"`
	TakenOver *uint64 `json:"takenOver"`
	Failed    *uint64 `json:"failed"`
}

type drainReplication struct {
	PendingWAL      *uint64 `json:"pendingWal"`
	UnderReplicated *uint64 `json:"underReplicated"`
}

type drainFailure struct {
	Code           string  `json:"code"`
	BlockingReason *string `json:"blockingReason"`
}

func (response drainResponse) dto() (rolloutapp.DrainObservationDTO, error) {
	nodeID, err := parseNodeID(response.NodeID)
	if err != nil || response.OperationID == "" || response.Phase == "" || response.State == "" || response.SafeToStop == nil ||
		response.Connections == nil || response.Connections.Initial == nil || response.Connections.Remaining == nil ||
		response.Sessions == nil || response.Sessions.Ready == nil || response.Sessions.TakenOver == nil || response.Sessions.Failed == nil ||
		response.Replication == nil || response.Replication.PendingWAL == nil || response.Replication.UnderReplicated == nil {
		return rolloutapp.DrainObservationDTO{}, errors.New("drain response omitted required safety evidence")
	}
	startedAt, err := time.Parse(time.RFC3339Nano, response.StartedAt)
	if err != nil {
		return rolloutapp.DrainObservationDTO{}, errors.New("drain response has invalid start time")
	}
	deadlineAt, err := time.Parse(time.RFC3339Nano, response.DeadlineAt)
	if err != nil || !deadlineAt.After(startedAt) {
		return rolloutapp.DrainObservationDTO{}, errors.New("drain response has invalid deadline")
	}
	observation := rolloutapp.DrainObservationDTO{
		OperationID: response.OperationID, NodeID: nodeID, State: response.State, Phase: response.Phase,
		StartedAt: startedAt.UTC(), DeadlineAt: deadlineAt.UTC(),
		ConnectionsInitial: *response.Connections.Initial, ConnectionsRemaining: *response.Connections.Remaining,
		SessionsReady: *response.Sessions.Ready, SessionsTakenOver: *response.Sessions.TakenOver, SessionsFailed: *response.Sessions.Failed,
		PendingWAL: *response.Replication.PendingWAL, UnderReplicated: *response.Replication.UnderReplicated,
		SafeToStop: new(*response.SafeToStop), FailurePresent: response.Failure != nil,
	}
	if response.Failure != nil {
		observation.FailureCode = response.Failure.Code
		if response.Failure.BlockingReason != nil {
			observation.BlockingReason = *response.Failure.BlockingReason
		}
	}
	return observation, nil
}

func parseNodeID(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != value {
		return 0, errors.New("node ID is not canonical")
	}
	return uint32(parsed), nil
}

func managementURL(endpoint, namespace string) (*url.URL, error) {
	if namespace == "" || len(validation.IsDNS1123Label(namespace)) != 0 {
		return nil, errors.New("management namespace is invalid")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.Port() != strconv.Itoa(int(managementServicePort)) {
		return nil, errors.New("management URL must use the in-cluster management Service")
	}
	serviceHost := managementServiceName + "." + namespace + ".svc"
	if parsed.Hostname() != serviceHost && parsed.Hostname() != serviceHost+".cluster.local" {
		return nil, errors.New("management URL must resolve to the same-namespace management Service")
	}
	return parsed, nil
}

func validAPIKey(token string) bool {
	if token == "" || strings.TrimSpace(token) != token {
		return false
	}
	for _, character := range token {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

var _ rolloutapp.Broker = (*ManagementClient)(nil)
