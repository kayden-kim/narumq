package rollout

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const mgmtTestEndpoint = "http://narumq-management.ns.svc:8080"

// mgmtRecorder remembers every request the management API handler received.
type mgmtRecorder struct {
	mu       sync.Mutex
	requests []mgmtRecorded
}

type mgmtRecorded struct {
	method, path string
	header       http.Header
	body         string
}

func (recorder *mgmtRecorder) record(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.requests = append(recorder.requests, mgmtRecorded{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: string(body)})
}

func (recorder *mgmtRecorder) all() []mgmtRecorded {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]mgmtRecorded(nil), recorder.requests...)
}

// newMgmtClient returns a ManagementClient whose transport sends in-cluster Service URLs to handler.
func newMgmtClient(t *testing.T, handler http.HandlerFunc) (*ManagementClient, *mgmtRecorder) {
	t.Helper()
	recorder := &mgmtRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.record(r)
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	mgmt := NewManagementClient()
	mgmt.client.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		r.URL.Scheme, r.URL.Host = target.Scheme, target.Host
		return transport.RoundTrip(r)
	})
	return mgmt, recorder
}

func mgmtDrainBody(mutate func(map[string]any)) map[string]any {
	body := map[string]any{
		"operationId": "op-1", "nodeId": "1", "state": "DRAINING", "phase": "TRANSFERRING_SESSIONS",
		"startedAt": "2026-10-14T12:00:00Z", "deadlineAt": "2026-10-14T12:10:00Z",
		"connections": map[string]any{"initial": 10, "remaining": 3},
		"sessions":    map[string]any{"stateReadyForTakeover": 5, "takenOver": 4, "failed": 1},
		"replication": map[string]any{"pendingWal": 2, "underReplicated": 1},
		"safeToStop":  false,
	}
	if mutate != nil {
		mutate(body)
	}
	return body
}

func mgmtWriteData(w http.ResponseWriter, data any) {
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
}

func TestManagementClientSendsCredentialAndIdempotencyOnlyWhereRequired(t *testing.T) {
	mgmt, recorder := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) {
		mgmtWriteData(w, mgmtDrainBody(nil))
	})
	ctx := context.Background()
	const token = "operator-secret-key"

	if _, err := mgmt.StartDrain(ctx, "ns", mgmtTestEndpoint+"/", token, 1, "idem-start"); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	if _, err := mgmt.CancelDrain(ctx, "ns", mgmtTestEndpoint, token, 1, "idem-cancel"); err != nil {
		t.Fatalf("CancelDrain: %v", err)
	}
	if _, _, err := mgmt.GetDrain(ctx, "ns", mgmtTestEndpoint, token, 1, ""); err != nil {
		t.Fatalf("GetDrain: %v", err)
	}

	requests := recorder.all()
	if len(requests) != 3 {
		t.Fatalf("requests = %+v", requests)
	}
	start, cancel, get := requests[0], requests[1], requests[2]
	if start.method != http.MethodPost || start.path != "/api/v1/nodes/1/drain" || start.body != "{}" ||
		start.header.Get("Content-Type") != "application/json" || start.header.Get("Idempotency-Key") != "idem-start" {
		t.Errorf("start request = %+v", start)
	}
	if cancel.method != http.MethodDelete || cancel.path != "/api/v1/nodes/1/drain" || cancel.body != "" ||
		cancel.header.Get("Content-Type") != "" || cancel.header.Get("Idempotency-Key") != "idem-cancel" {
		t.Errorf("cancel request = %+v", cancel)
	}
	if get.method != http.MethodGet || get.body != "" || get.header.Get("Content-Type") != "" || get.header.Get("Idempotency-Key") != "" {
		t.Errorf("get request = %+v", get)
	}
	for _, request := range requests {
		if request.header.Get("X-API-Key") != token || request.header.Get("Accept") != "application/json" || request.header.Get("Authorization") != "" {
			t.Errorf("%s credentials/headers = %v", request.method, request.header)
		}
	}
}

func TestManagementClientDoesNotSendCredentialsToInvalidTargets(t *testing.T) {
	mgmt, recorder := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) { mgmtWriteData(w, mgmtDrainBody(nil)) })
	ctx := context.Background()
	cases := []struct {
		name, namespace, endpoint, token string
	}{
		{"endpoint on another host", "ns", "http://evil.test:8080", "key"},
		{"endpoint in another namespace", "ns", "http://narumq-management.other.svc:8080", "key"},
		{"endpoint with path", "ns", mgmtTestEndpoint + "/extra", "key"},
		{"empty namespace", "", mgmtTestEndpoint, "key"},
		{"empty token", "ns", mgmtTestEndpoint, ""},
		{"token with leading space", "ns", mgmtTestEndpoint, " key"},
		{"token with trailing space", "ns", mgmtTestEndpoint, "key "},
		{"token with embedded space", "ns", mgmtTestEndpoint, "k ey"},
		{"token with control character", "ns", mgmtTestEndpoint, "k\tey"},
		{"token with DEL", "ns", mgmtTestEndpoint, "k\x7fey"},
	}
	for _, tc := range cases {
		if _, err := mgmt.StartDrain(ctx, tc.namespace, tc.endpoint, tc.token, 1, "idem"); err == nil {
			t.Errorf("%s: request was accepted", tc.name)
		}
	}
	if got := recorder.all(); len(got) != 0 {
		t.Fatalf("requests reached the server despite invalid target/credential: %+v", got)
	}
}

func TestManagementURLAcceptsOnlySameNamespaceServiceOnPort8080(t *testing.T) {
	accepted := []string{
		mgmtTestEndpoint, mgmtTestEndpoint + "/",
		"http://narumq-management.ns.svc.cluster.local:8080",
	}
	for _, endpoint := range accepted {
		if _, err := managementURL(endpoint, "ns"); err != nil {
			t.Errorf("%q rejected: %v", endpoint, err)
		}
	}
	rejected := map[string]string{
		"wrong port":        "http://narumq-management.ns.svc:9090",
		"no port":           "http://narumq-management.ns.svc",
		"https":             "https://narumq-management.ns.svc:8080",
		"userinfo":          "http://user@narumq-management.ns.svc:8080",
		"query":             "http://narumq-management.ns.svc:8080?x=1",
		"fragment":          "http://narumq-management.ns.svc:8080#x",
		"path":              "http://narumq-management.ns.svc:8080/api",
		"other namespace":   "http://narumq-management.other.svc:8080",
		"other service":     "http://other.ns.svc:8080",
		"external hostname": "http://narumq-management.ns.svc.evil.test:8080",
		"unparseable":       "http://[::1",
	}
	for name, endpoint := range rejected {
		if _, err := managementURL(endpoint, "ns"); err == nil {
			t.Errorf("%s: %q accepted", name, endpoint)
		}
	}
	for _, namespace := range []string{"", "Bad_NS", "UPPER"} {
		if _, err := managementURL(mgmtTestEndpoint, namespace); err == nil {
			t.Errorf("namespace %q accepted", namespace)
		}
	}
}

func TestManagementClientFailsClosedOnTransportAndHTTPErrors(t *testing.T) {
	const token = "very-secret-token"
	ctx := context.Background()

	t.Run("transport failure does not leak the credential", func(t *testing.T) {
		mgmt := NewManagementClient()
		mgmt.client.Transport = roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("dial failed with " + token)
		})
		_, err := mgmt.StartDrain(ctx, "ns", mgmtTestEndpoint, token, 1, "idem")
		if err == nil {
			t.Fatal("transport failure was not reported")
		}
		if strings.Contains(err.Error(), token) {
			t.Fatalf("error leaks the API key: %v", err)
		}
	})

	t.Run("nil context cannot build a request", func(t *testing.T) {
		mgmt, recorder := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) { mgmtWriteData(w, mgmtDrainBody(nil)) })
		var noContext context.Context
		if _, err := mgmt.StartDrain(noContext, "ns", mgmtTestEndpoint, token, 1, "idem"); err == nil {
			t.Fatal("request built with a nil context")
		}
		if got := recorder.all(); len(got) != 0 {
			t.Fatalf("request was sent: %+v", got)
		}
	})

	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			mgmt, _ := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) })
			_, err := mgmt.StartDrain(ctx, "ns", mgmtTestEndpoint, token, 1, "idem")
			var statusErr *httpStatusError
			if !errors.As(err, &statusErr) || statusErr.status != status {
				t.Fatalf("StartDrain error = %v, want httpStatusError %d", err, status)
			}
			if _, err := mgmt.ClusterFeatures(ctx, "ns", mgmtTestEndpoint, token); !errors.As(err, &statusErr) || statusErr.status != status {
				t.Fatalf("ClusterFeatures error = %v, want httpStatusError %d", err, status)
			}
			if _, err := mgmt.CancelDrain(ctx, "ns", mgmtTestEndpoint, token, 1, "idem"); !errors.As(err, &statusErr) || statusErr.status != status {
				t.Fatalf("CancelDrain error = %v, want httpStatusError %d", err, status)
			}
			_, found, err := mgmt.GetDrain(ctx, "ns", mgmtTestEndpoint, token, 1, "")
			if status == http.StatusNotFound {
				if err != nil || found {
					t.Fatalf("GetDrain on 404 = found %v, %v; want absent without error", found, err)
				}
				return
			}
			if !errors.As(err, &statusErr) || statusErr.status != status || found {
				t.Fatalf("GetDrain error = %v found=%v, want httpStatusError %d", err, found, status)
			}
		})
	}

	t.Run("redirects are not followed", func(t *testing.T) {
		mgmt, recorder := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
		})
		_, err := mgmt.StartDrain(ctx, "ns", mgmtTestEndpoint, token, 1, "idem")
		var statusErr *httpStatusError
		if !errors.As(err, &statusErr) || statusErr.status != http.StatusTemporaryRedirect {
			t.Fatalf("redirect error = %v", err)
		}
		if got := recorder.all(); len(got) != 1 {
			t.Fatalf("redirect was followed: %+v", got)
		}
	})
}

func TestManagementClientRejectsMalformedEnvelopes(t *testing.T) {
	ctx := context.Background()
	bodies := map[string]string{
		"not JSON":           "<html>",
		"empty object":       `{}`,
		"null data":          `{"data":null}`,
		"data of wrong type": `{"data":"drain"}`,
		"truncated oversize": `{"data":{"operationId":"` + strings.Repeat("a", maxManagementBody) + `"}}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			mgmt, _ := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) })
			if _, err := mgmt.StartDrain(ctx, "ns", mgmtTestEndpoint, "key", 1, "idem"); err == nil {
				t.Error("StartDrain accepted malformed envelope")
			}
			if _, err := mgmt.ClusterFeatures(ctx, "ns", mgmtTestEndpoint, "key"); err == nil {
				t.Error("ClusterFeatures accepted malformed envelope")
			}
			if _, found, err := mgmt.GetDrain(ctx, "ns", mgmtTestEndpoint, "key", 1, ""); err == nil || found {
				t.Errorf("GetDrain = found %v, %v; want error", found, err)
			}
		})
	}
}

func TestManagementClientRejectsDrainEvidenceForAnotherNode(t *testing.T) {
	mgmt, _ := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) {
		mgmtWriteData(w, mgmtDrainBody(func(body map[string]any) { body["nodeId"] = "2" }))
	})
	ctx := context.Background()
	if _, err := mgmt.StartDrain(ctx, "ns", mgmtTestEndpoint, "key", 1, "idem"); err == nil {
		t.Error("StartDrain accepted evidence for node 2 when node 1 was requested")
	}
	if _, err := mgmt.CancelDrain(ctx, "ns", mgmtTestEndpoint, "key", 1, "idem"); err == nil {
		t.Error("CancelDrain accepted evidence for node 2 when node 1 was requested")
	}
	if obs, found, err := mgmt.GetDrain(ctx, "ns", mgmtTestEndpoint, "key", 1, ""); err == nil || found || obs.OperationID != "" {
		t.Errorf("GetDrain = %+v, %v, %v; want rejection", obs, found, err)
	}
	if obs, err := mgmt.StartDrain(ctx, "ns", mgmtTestEndpoint, "key", 2, "idem"); err != nil || obs.NodeID != 2 {
		t.Errorf("matching node rejected: %+v, %v", obs, err)
	}
}

func TestManagementClientClusterFeaturesRequiresCompleteMembershipEvidence(t *testing.T) {
	member := func(id, version string, features any) map[string]any {
		return map[string]any{"nodeId": id, "releaseVersion": version, "features": features}
	}
	serve := func(t *testing.T, data map[string]any) (*ManagementClient, error) {
		t.Helper()
		mgmt, _ := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) { mgmtWriteData(w, data) })
		_, err := mgmt.ClusterFeatures(context.Background(), "ns", mgmtTestEndpoint, "key")
		return mgmt, err
	}

	rejected := map[string]map[string]any{
		"non-canonical node id": {"activeFeatures": []string{}, "members": []any{member("01", "1.0.0", []string{})}},
		"zero node id":          {"activeFeatures": []string{}, "members": []any{member("0", "1.0.0", []string{})}},
		"non-numeric node id":   {"activeFeatures": []string{}, "members": []any{member("a", "1.0.0", []string{})}},
		"empty release version": {"activeFeatures": []string{}, "members": []any{member("1", "", []string{})}},
		"features omitted":      {"activeFeatures": []string{}, "members": []any{member("1", "1.0.0", nil)}},
		"activeFeatures absent": {"members": []any{member("1", "1.0.0", []string{})}},
		"activeFeatures null":   {"activeFeatures": nil, "members": []any{}},
	}
	for name, data := range rejected {
		if _, err := serve(t, data); err == nil {
			t.Errorf("%s: incomplete cluster feature response accepted", name)
		}
	}

	mgmt, _ := newMgmtClient(t, func(w http.ResponseWriter, r *http.Request) {
		mgmtWriteData(w, map[string]any{
			"activeFeatures": []string{"a"},
			"members":        []any{member("2", "1.0.0", []string{"a", "b"}), member("1", "1.1.0", []string{})},
		})
	})
	features, err := mgmt.ClusterFeatures(context.Background(), "ns", mgmtTestEndpoint, "key")
	if err != nil {
		t.Fatalf("ClusterFeatures: %v", err)
	}
	if len(features.ActiveFeatures) != 1 || features.ActiveFeatures[0] != "a" || len(features.Members) != 2 ||
		features.Members[0].NodeID != 2 || features.Members[0].ReleaseVersion != "1.0.0" || len(features.Members[0].Features) != 2 ||
		features.Members[1].NodeID != 1 || features.Members[1].ReleaseVersion != "1.1.0" || len(features.Members[1].Features) != 0 {
		t.Fatalf("features = %+v", features)
	}
}

func TestDrainResponseDTORequiresEverySafetyField(t *testing.T) {
	decode := func(t *testing.T, mutate func(map[string]any)) drainResponse {
		t.Helper()
		raw, err := json.Marshal(mgmtDrainBody(mutate))
		if err != nil {
			t.Fatal(err)
		}
		var response drainResponse
		if err := json.Unmarshal(raw, &response); err != nil {
			t.Fatal(err)
		}
		return response
	}

	rejected := map[string]func(map[string]any){
		"node id non-canonical":      func(b map[string]any) { b["nodeId"] = "01" },
		"node id zero":               func(b map[string]any) { b["nodeId"] = "0" },
		"operation id empty":         func(b map[string]any) { b["operationId"] = "" },
		"phase empty":                func(b map[string]any) { b["phase"] = "" },
		"state empty":                func(b map[string]any) { b["state"] = "" },
		"safeToStop missing":         func(b map[string]any) { delete(b, "safeToStop") },
		"connections missing":        func(b map[string]any) { delete(b, "connections") },
		"connections.initial gone":   func(b map[string]any) { b["connections"] = map[string]any{"remaining": 0} },
		"connections.remaining gone": func(b map[string]any) { b["connections"] = map[string]any{"initial": 0} },
		"sessions missing":           func(b map[string]any) { delete(b, "sessions") },
		"sessions.ready gone":        func(b map[string]any) { b["sessions"] = map[string]any{"takenOver": 0, "failed": 0} },
		"sessions.takenOver gone":    func(b map[string]any) { b["sessions"] = map[string]any{"stateReadyForTakeover": 0, "failed": 0} },
		"sessions.failed gone":       func(b map[string]any) { b["sessions"] = map[string]any{"stateReadyForTakeover": 0, "takenOver": 0} },
		"replication missing":        func(b map[string]any) { delete(b, "replication") },
		"replication.pendingWal gone": func(b map[string]any) {
			b["replication"] = map[string]any{"underReplicated": 0}
		},
		"replication.underReplicated gone": func(b map[string]any) {
			b["replication"] = map[string]any{"pendingWal": 0}
		},
		"startedAt unparsable":  func(b map[string]any) { b["startedAt"] = "yesterday" },
		"deadlineAt unparsable": func(b map[string]any) { b["deadlineAt"] = "" },
		"deadline equals start": func(b map[string]any) { b["deadlineAt"] = b["startedAt"] },
		"deadline before start": func(b map[string]any) { b["deadlineAt"] = "2026-10-14T11:59:59Z" },
	}
	for name, mutate := range rejected {
		if observation, err := decode(t, mutate).dto(); err == nil {
			t.Errorf("%s: incomplete drain evidence accepted: %+v", name, observation)
		}
	}

	observation, err := decode(t, func(b map[string]any) {
		b["startedAt"] = "2026-10-14T21:00:00+09:00"
		b["deadlineAt"] = "2026-10-14T21:10:00.5+09:00"
	}).dto()
	if err != nil {
		t.Fatalf("valid drain evidence rejected: %v", err)
	}
	if observation.OperationID != "op-1" || observation.NodeID != 1 || observation.State != "DRAINING" || observation.Phase != "TRANSFERRING_SESSIONS" ||
		observation.ConnectionsInitial != 10 || observation.ConnectionsRemaining != 3 ||
		observation.SessionsReady != 5 || observation.SessionsTakenOver != 4 || observation.SessionsFailed != 1 ||
		observation.PendingWAL != 2 || observation.UnderReplicated != 1 ||
		observation.SafeToStop == nil || *observation.SafeToStop || observation.FailurePresent ||
		observation.FailureCode != "" || observation.BlockingReason != "" {
		t.Fatalf("observation = %+v", observation)
	}
	if !observation.StartedAt.Equal(time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)) || observation.StartedAt.Location() != time.UTC ||
		!observation.DeadlineAt.Equal(time.Date(2026, 10, 14, 12, 10, 0, 500_000_000, time.UTC)) || observation.DeadlineAt.Location() != time.UTC {
		t.Fatalf("times not normalized to UTC: %v %v", observation.StartedAt, observation.DeadlineAt)
	}
}

func TestDrainResponseDTOReportsFailureDetails(t *testing.T) {
	decode := func(t *testing.T, failure map[string]any) drainResponse {
		t.Helper()
		raw, err := json.Marshal(mgmtDrainBody(func(b map[string]any) { b["failure"] = failure }))
		if err != nil {
			t.Fatal(err)
		}
		var response drainResponse
		if err := json.Unmarshal(raw, &response); err != nil {
			t.Fatal(err)
		}
		return response
	}

	withReason, err := decode(t, map[string]any{"code": "TRANSFER_TIMEOUT", "blockingReason": "pending WAL"}).dto()
	if err != nil {
		t.Fatal(err)
	}
	if !withReason.FailurePresent || withReason.FailureCode != "TRANSFER_TIMEOUT" || withReason.BlockingReason != "pending WAL" {
		t.Errorf("failure with reason = %+v", withReason)
	}

	withoutReason, err := decode(t, map[string]any{"code": "TRANSFER_TIMEOUT"}).dto()
	if err != nil {
		t.Fatal(err)
	}
	if !withoutReason.FailurePresent || withoutReason.FailureCode != "TRANSFER_TIMEOUT" || withoutReason.BlockingReason != "" {
		t.Errorf("failure without reason = %+v", withoutReason)
	}
}
