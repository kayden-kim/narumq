package tcp

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	managementports "narumq/internal/application/ports/management"
	metricsports "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
)

// peerObsMarshalMetrics encodes data verbatim so the reader's own validation is what rejects it.
func peerObsMarshalMetrics(t *testing.T, data observationports.NodeMetricsDataDTO) []byte {
	t.Helper()
	body, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal metrics body: %v", err)
	}
	return body
}

func requirePeerQueryCode(t *testing.T, err error, want observationports.QueryErrorCode) {
	t.Helper()
	var queryErr *observationports.PeerQueryError
	if !errors.As(err, &queryErr) || queryErr.Code != want {
		t.Fatalf("error = %v, want peer query code %q", err, want)
	}
}

// peerObsQueryService serves fixed local snapshots and records which node each query asked for.
type peerObsQueryService struct {
	mu         sync.Mutex
	node       observationports.NodeDataDTO
	nodeErr    error
	metrics    observationports.NodeMetricsDataDTO
	metricsErr error
	asked      []string
}

func (service *peerObsQueryService) GetCluster(context.Context) (observationports.ClusterDataDTO, error) {
	return observationports.ClusterDataDTO{}, errors.New("cluster queries are not served to peers")
}

func (service *peerObsQueryService) GetNode(_ context.Context, nodeID string) (observationports.NodeDataDTO, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.asked = append(service.asked, nodeID)
	return service.node, service.nodeErr
}

func (service *peerObsQueryService) ListNodes(context.Context, observationports.NodeFilter) ([]observationports.NodeDataDTO, *managementports.PageDTO, error) {
	return nil, nil, errors.New("node lists are not served to peers")
}

func (service *peerObsQueryService) GetNodeMetrics(_ context.Context, nodeID string) (observationports.NodeMetricsDataDTO, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.asked = append(service.asked, nodeID)
	return service.metrics, service.metricsErr
}

func (service *peerObsQueryService) CollectMetrics(context.Context, observationports.MetricsMode) (observationports.MetricsCollectionDataDTO, error) {
	return observationports.MetricsCollectionDataDTO{}, errors.New("cluster metrics are not served to peers")
}

func (service *peerObsQueryService) askedNodes() []string {
	service.mu.Lock()
	defer service.mu.Unlock()
	return append([]string(nil), service.asked...)
}

func peerObsNodeData(observed time.Time) observationports.NodeDataDTO {
	return observationports.NodeDataDTO{
		NodeID: "2", MembershipPhase: observationports.MembershipPhaseVoter,
		QueryStatus: observationports.QueryStatusOK, ObservedAt: &observed,
		Version: new("1.0.0"), UptimeMillis: new(int64(1500)),
		State: new(observationports.NodeStateReady), Ready: new(true),
		ReadinessReason: new("ok"), Role: new("leader"),
		LastAppliedIndex: new(uint64(42)), ConnectionCount: new(uint64(3)),
		SessionCount: new(uint64(5)), StoragePressure: new(false),
		ReplicationHealth: new("healthy"), LastErrorCode: new("none"),
	}
}

func peerObsMetricsData(observed, started time.Time) observationports.NodeMetricsDataDTO {
	return observationports.NodeMetricsDataDTO{
		NodeID: "2", QueryStatus: observationports.QueryStatusOK, ObservedAt: &observed, StartedAt: &started,
		Families: []observationports.MetricFamilyDTO{
			{
				Name: "narumq_connections", Help: "Open connections", Type: "gauge", Scope: "node", Aggregation: "sum",
				LabelNames: []string{"listener"},
				Samples: []observationports.MetricSampleDTO{{
					Name: "narumq_connections", Labels: []observationports.MetricLabelDTO{{Name: "listener", Value: "tcp"}},
					ValueKind: "integer", Value: "18446744073709551615",
				}},
			},
			{
				Name: "narumq_latency_seconds", Help: "Latency", Type: "gauge", Scope: "node", Aggregation: "max",
				LabelNames: []string{"stage"},
				Samples: []observationports.MetricSampleDTO{{
					Name: "narumq_latency_seconds", Labels: []observationports.MetricLabelDTO{{Name: "stage", Value: "ack"}},
					ValueKind: "float", Value: "0.25",
				}},
			},
		},
	}
}

// peerReaderFixture runs a querying member (1) and a serving member (2) over real mTLS transports.
type peerReaderFixture struct {
	reader  *PeerObservabilityReader
	client  *DynamicTCPPeerTransport
	calls   atomic.Int32
	mu      sync.Mutex
	handler PeerObservabilityHandler
}

func (fixture *peerReaderFixture) serve(ctx context.Context, kind PeerObservabilityKind) ([]byte, error) {
	fixture.calls.Add(1)
	fixture.mu.Lock()
	handler := fixture.handler
	fixture.mu.Unlock()
	return handler(ctx, kind)
}

func (fixture *peerReaderFixture) respond(handler PeerObservabilityHandler) {
	fixture.mu.Lock()
	fixture.handler = handler
	fixture.mu.Unlock()
}

func (fixture *peerReaderFixture) respondBody(body string) {
	fixture.respond(func(context.Context, PeerObservabilityKind) ([]byte, error) { return []byte(body), nil })
}

func newPeerReaderFixture(t *testing.T) *peerReaderFixture {
	t.Helper()
	fixture := &peerReaderFixture{}
	server, client := cptNewNode(t, "server.example"), cptNewNode(t, "client.example")
	cptTrustEachOther(server, client)
	serverListener, clientListener := cptListen(t), cptListen(t)
	serverIdentity := peerObservabilityTestIdentity(server, 2, serverListener.Addr().String())
	clientIdentity := peerObservabilityTestIdentity(client, 1, clientListener.Addr().String())
	serverTransport := newPeerObservabilityTestTransport(t, server, serverListener, serverIdentity, []PeerIdentityDTO{clientIdentity}, fixture.serve)
	fixture.client = newPeerObservabilityTestTransport(t, client, clientListener, clientIdentity, []PeerIdentityDTO{serverIdentity}, nil)
	startPeerObservabilityTestTransport(t, serverTransport)
	startPeerObservabilityTestTransport(t, fixture.client)
	fixture.reader = NewPeerObservabilityReader(fixture.client)
	return fixture
}

func TestPeerObservabilityHandlerServesOnlyTheLocalNode(t *testing.T) {
	observed := time.Date(2026, 10, 17, 1, 2, 3, 0, time.UTC)
	started := observed.Add(-time.Hour)
	ctx := context.Background()

	t.Run("node snapshot is the local node's data", func(t *testing.T) {
		service := &peerObsQueryService{node: peerObsNodeData(observed)}
		body, err := NewPeerObservabilityHandler(service, "2")(ctx, PeerObservabilityNode)
		if err != nil {
			t.Fatalf("serve node snapshot: %v", err)
		}
		var decoded observationports.NodeDataDTO
		if err := decodePeerObservabilityBody(body, &decoded); err != nil {
			t.Fatalf("decode served body: %v", err)
		}
		if decoded.NodeID != "2" || decoded.QueryStatus != observationports.QueryStatusOK || decoded.Ready == nil || !*decoded.Ready {
			t.Fatalf("served node snapshot = %+v", decoded)
		}
		if asked := service.askedNodes(); !reflect.DeepEqual(asked, []string{"2"}) {
			t.Fatalf("service was asked for nodes %v, want only the local node", asked)
		}
	})
	t.Run("metrics snapshot is the local node's data", func(t *testing.T) {
		service := &peerObsQueryService{metrics: peerObsMetricsData(observed, started)}
		body, err := NewPeerObservabilityHandler(service, "2")(ctx, PeerObservabilityMetrics)
		if err != nil {
			t.Fatalf("serve metrics snapshot: %v", err)
		}
		var decoded observationports.NodeMetricsDataDTO
		if err := decodePeerObservabilityBody(body, &decoded); err != nil {
			t.Fatalf("decode served body: %v", err)
		}
		if decoded.NodeID != "2" || len(decoded.Families) != 2 {
			t.Fatalf("served metrics snapshot = %+v", decoded)
		}
		if asked := service.askedNodes(); !reflect.DeepEqual(asked, []string{"2"}) {
			t.Fatalf("service was asked for nodes %v, want only the local node", asked)
		}
	})
	t.Run("service errors are returned and nothing is served", func(t *testing.T) {
		failure := errors.New("query backend failed")
		service := &peerObsQueryService{nodeErr: failure, metricsErr: failure}
		handler := NewPeerObservabilityHandler(service, "2")
		for _, kind := range []PeerObservabilityKind{PeerObservabilityNode, PeerObservabilityMetrics} {
			body, err := handler(ctx, kind)
			if !errors.Is(err, failure) || body != nil {
				t.Fatalf("kind %d = %q, %v; want the backend error and no body", kind, body, err)
			}
		}
	})
	t.Run("an unavailable local snapshot is not served", func(t *testing.T) {
		unavailable := peerObsNodeData(observed)
		unavailable.QueryStatus = observationports.QueryStatusError
		unavailableMetrics := peerObsMetricsData(observed, started)
		unavailableMetrics.QueryStatus = observationports.QueryStatusError
		handler := NewPeerObservabilityHandler(&peerObsQueryService{node: unavailable, metrics: unavailableMetrics}, "2")
		for _, kind := range []PeerObservabilityKind{PeerObservabilityNode, PeerObservabilityMetrics} {
			body, err := handler(ctx, kind)
			if err == nil || body != nil {
				t.Fatalf("kind %d served an unavailable snapshot: %q, %v", kind, body, err)
			}
		}
	})
	t.Run("missing service or local identity is refused", func(t *testing.T) {
		for name, handler := range map[string]PeerObservabilityHandler{
			"nil service":  NewPeerObservabilityHandler(nil, "2"),
			"empty nodeID": NewPeerObservabilityHandler(&peerObsQueryService{node: peerObsNodeData(observed)}, ""),
		} {
			if body, err := handler(ctx, PeerObservabilityNode); err == nil || body != nil {
				t.Errorf("%s served %q, %v", name, body, err)
			}
		}
	})
	t.Run("unknown kind is an invalid peer message", func(t *testing.T) {
		service := &peerObsQueryService{node: peerObsNodeData(observed)}
		body, err := NewPeerObservabilityHandler(service, "2")(ctx, PeerObservabilityKind(9))
		if !errors.Is(err, ErrInvalidPeerMessage) || body != nil {
			t.Fatalf("unknown kind = %q, %v; want ErrInvalidPeerMessage", body, err)
		}
		if asked := service.askedNodes(); len(asked) != 0 {
			t.Fatalf("unknown kind queried the service for %v", asked)
		}
	})
}

func TestPeerObservabilityReaderReturnsPeerSnapshotsOverMTLS(t *testing.T) {
	fixture := newPeerReaderFixture(t)
	observed := time.Date(2026, 10, 17, 1, 2, 3, 0, time.UTC)
	started := observed.Add(-time.Hour)
	service := &peerObsQueryService{node: peerObsNodeData(observed), metrics: peerObsMetricsData(observed, started)}
	fixture.respond(NewPeerObservabilityHandler(service, "2"))

	node, err := fixture.reader.QueryNodeSnapshot(context.Background(), "2")
	if err != nil {
		t.Fatalf("QueryNodeSnapshot: %v", err)
	}
	wantNode := peerObsNodeData(observed)
	if !node.ObservedAt.Equal(observed) {
		t.Fatalf("node ObservedAt = %v, want %v", node.ObservedAt, observed)
	}
	node.ObservedAt = observed
	if want := (observationports.NodeSnapshotDTO{
		ObservedAt: observed, Version: wantNode.Version, UptimeMillis: wantNode.UptimeMillis, State: wantNode.State,
		Ready: wantNode.Ready, ReadinessReason: wantNode.ReadinessReason, Role: wantNode.Role,
		LastAppliedIndex: wantNode.LastAppliedIndex, ConnectionCount: wantNode.ConnectionCount,
		SessionCount: wantNode.SessionCount, StoragePressure: wantNode.StoragePressure,
		ReplicationHealth: wantNode.ReplicationHealth, LastErrorCode: wantNode.LastErrorCode,
	}); !reflect.DeepEqual(node, want) {
		t.Fatalf("node snapshot = %+v, want %+v", node, want)
	}

	metrics, err := fixture.reader.QueryMetricsSnapshot(context.Background(), "2")
	if err != nil {
		t.Fatalf("QueryMetricsSnapshot: %v", err)
	}
	if !metrics.ObservedAt.Equal(observed) || !metrics.StartedAt.Equal(started) {
		t.Fatalf("metrics boundaries = %v / %v, want %v / %v", metrics.ObservedAt, metrics.StartedAt, observed, started)
	}
	wantSnapshot := metricsports.MetricsSnapshotDTO{Families: []metricsports.MetricFamilyDTO{
		{
			Descriptor: metricsports.MetricDescriptorDTO{
				Name: "narumq_connections", Help: "Open connections", Type: metricsports.MetricTypeGauge,
				Scope: metricsports.MetricScope("node"), Aggregation: metricsports.MetricAggregation("sum"),
				LabelNames: []string{"listener"},
			},
			Samples: []metricsports.MetricSampleDTO{{
				Name: "narumq_connections", Labels: []metricsports.MetricLabelDTO{{Name: "listener", Value: "tcp"}},
				Value: metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: math.MaxUint64},
			}},
		},
		{
			Descriptor: metricsports.MetricDescriptorDTO{
				Name: "narumq_latency_seconds", Help: "Latency", Type: metricsports.MetricTypeGauge,
				Scope: metricsports.MetricScope("node"), Aggregation: metricsports.MetricAggregation("max"),
				LabelNames: []string{"stage"},
			},
			Samples: []metricsports.MetricSampleDTO{{
				Name: "narumq_latency_seconds", Labels: []metricsports.MetricLabelDTO{{Name: "stage", Value: "ack"}},
				Value: metricsports.MetricValueDTO{Kind: metricsports.MetricValueFloat, Float64: 0.25},
			}},
		},
	}}
	if !reflect.DeepEqual(metrics.Snapshot, wantSnapshot) {
		t.Fatalf("metrics snapshot = %+v, want %+v", metrics.Snapshot, wantSnapshot)
	}
	if asked := service.askedNodes(); !reflect.DeepEqual(asked, []string{"2", "2"}) {
		t.Fatalf("serving node answered for %v, want only itself", asked)
	}
}

func TestPeerObservabilityReaderRejectsMalformedMemberIDWithoutQuerying(t *testing.T) {
	fixture := newPeerReaderFixture(t)
	fixture.respondBody(`{"nodeId":"2"}`)
	for _, memberID := range []string{"", "abc", "0", "02", "+2", "-2", " 2", "2 ", "18446744073709551616"} {
		if _, err := fixture.reader.QueryNodeSnapshot(context.Background(), memberID); err == nil {
			t.Errorf("node query for %q succeeded", memberID)
		} else {
			requirePeerQueryCode(t, err, observationports.QueryErrorInvalidResponse)
		}
		if _, err := fixture.reader.QueryMetricsSnapshot(context.Background(), memberID); err == nil {
			t.Errorf("metrics query for %q succeeded", memberID)
		} else {
			requirePeerQueryCode(t, err, observationports.QueryErrorInvalidResponse)
		}
	}
	if calls := fixture.calls.Load(); calls != 0 {
		t.Fatalf("serving node was queried %d times for malformed member IDs", calls)
	}
}

func TestPeerObservabilityReaderRejectsInvalidNodeBodies(t *testing.T) {
	fixture := newPeerReaderFixture(t)
	observed := time.Date(2026, 10, 17, 1, 2, 3, 0, time.UTC)
	for name, body := range map[string]string{
		"node ID of another member":      `{"nodeId":"3","membershipPhase":"VOTER","queryStatus":"OK","queryErrorCode":null,"observedAt":"2026-10-17T01:02:03Z"}`,
		"query status not OK":            `{"nodeId":"2","membershipPhase":"VOTER","queryStatus":"ERROR","queryErrorCode":null,"observedAt":"2026-10-17T01:02:03Z"}`,
		"carries a query error code":     `{"nodeId":"2","membershipPhase":"VOTER","queryStatus":"OK","queryErrorCode":"node_query_failed","observedAt":"2026-10-17T01:02:03Z"}`,
		"no observation time":            `{"nodeId":"2","membershipPhase":"VOTER","queryStatus":"OK","queryErrorCode":null,"observedAt":null}`,
		"field outside the DTO":          `{"nodeId":"2","membershipPhase":"VOTER","queryStatus":"OK","queryErrorCode":null,"observedAt":"2026-10-17T01:02:03Z","extra":1}`,
		"field with the wrong JSON type": `{"nodeId":2,"queryStatus":"OK"}`,
	} {
		fixture.respondBody(body)
		snapshot, err := fixture.reader.QueryNodeSnapshot(context.Background(), "2")
		if err == nil {
			t.Errorf("%s: node snapshot was accepted: %+v", name, snapshot)
			continue
		}
		requirePeerQueryCode(t, err, observationports.QueryErrorInvalidResponse)
		if !snapshot.ObservedAt.IsZero() {
			t.Errorf("%s: a rejected snapshot leaked data: %+v", name, snapshot)
		}
	}
	// The same fixture still serves a valid body afterwards.
	fixture.respond(NewPeerObservabilityHandler(&peerObsQueryService{node: peerObsNodeData(observed)}, "2"))
	if _, err := fixture.reader.QueryNodeSnapshot(context.Background(), "2"); err != nil {
		t.Fatalf("valid node snapshot after rejected bodies: %v", err)
	}
}

func TestPeerObservabilityReaderRejectsInvalidMetricBodies(t *testing.T) {
	fixture := newPeerReaderFixture(t)
	observed := time.Date(2026, 10, 17, 1, 2, 3, 0, time.UTC)
	started := observed.Add(-time.Hour)
	mutate := func(change func(*observationports.NodeMetricsDataDTO)) PeerObservabilityHandler {
		data := peerObsMetricsData(observed, started)
		change(&data)
		body := peerObsMarshalMetrics(t, data)
		return func(context.Context, PeerObservabilityKind) ([]byte, error) { return body, nil }
	}
	for name, handler := range map[string]PeerObservabilityHandler{
		"node ID of another member": mutate(func(data *observationports.NodeMetricsDataDTO) { data.NodeID = "3" }),
		"query status not OK":       mutate(func(data *observationports.NodeMetricsDataDTO) { data.QueryStatus = observationports.QueryStatusError }),
		"carries a query error code": mutate(func(data *observationports.NodeMetricsDataDTO) {
			data.QueryErrorCode = new(observationports.QueryErrorFailed)
		}),
		"no observation time": mutate(func(data *observationports.NodeMetricsDataDTO) { data.ObservedAt = nil }),
		"no start time":       mutate(func(data *observationports.NodeMetricsDataDTO) { data.StartedAt = nil }),
		"no family list":      mutate(func(data *observationports.NodeMetricsDataDTO) { data.Families = nil }),
		"unknown sample value kind": mutate(func(data *observationports.NodeMetricsDataDTO) {
			data.Families[0].Samples[0].ValueKind = "string"
		}),
		"non-canonical integer": mutate(func(data *observationports.NodeMetricsDataDTO) {
			data.Families[0].Samples[0].Value = "007"
		}),
		"integer above 64 bits": mutate(func(data *observationports.NodeMetricsDataDTO) {
			data.Families[0].Samples[0].Value = "18446744073709551616"
		}),
		"NaN float": mutate(func(data *observationports.NodeMetricsDataDTO) {
			data.Families[1].Samples[0].Value = "NaN"
		}),
	} {
		fixture.respond(handler)
		snapshot, err := fixture.reader.QueryMetricsSnapshot(context.Background(), "2")
		if err == nil {
			t.Errorf("%s: metrics snapshot was accepted: %+v", name, snapshot)
			continue
		}
		requirePeerQueryCode(t, err, observationports.QueryErrorInvalidResponse)
		if len(snapshot.Snapshot.Families) != 0 {
			t.Errorf("%s: a rejected snapshot leaked families: %+v", name, snapshot)
		}
	}
	fixture.respond(mutate(func(data *observationports.NodeMetricsDataDTO) { data.Families = []observationports.MetricFamilyDTO{} }))
	snapshot, err := fixture.reader.QueryMetricsSnapshot(context.Background(), "2")
	if err != nil || snapshot.Snapshot.Families == nil || len(snapshot.Snapshot.Families) != 0 {
		t.Fatalf("empty family list = %+v, %v; want an accepted empty snapshot", snapshot, err)
	}
}

func TestPeerObservabilityReaderMapsTransportFailuresToQueryCodes(t *testing.T) {
	fixture := newPeerReaderFixture(t)
	fixture.respond(NewPeerObservabilityHandler(&peerObsQueryService{node: peerObsNodeData(time.Now().UTC())}, "2"))

	t.Run("expired deadline is a timeout", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		_, nodeErr := fixture.reader.QueryNodeSnapshot(ctx, "2")
		requirePeerQueryCode(t, nodeErr, observationports.QueryErrorTimeout)
		_, metricsErr := fixture.reader.QueryMetricsSnapshot(ctx, "2")
		requirePeerQueryCode(t, metricsErr, observationports.QueryErrorTimeout)
	})
	t.Run("canceled caller is unreachable, not a timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := fixture.reader.QueryNodeSnapshot(ctx, "2")
		requirePeerQueryCode(t, err, observationports.QueryErrorNodeUnreachable)
	})
	t.Run("member outside the committed set is unreachable", func(t *testing.T) {
		_, err := fixture.reader.QueryNodeSnapshot(context.Background(), "9")
		requirePeerQueryCode(t, err, observationports.QueryErrorNodeUnreachable)
		if calls := fixture.calls.Load(); calls != 0 {
			t.Fatalf("uncommitted member reached the serving node %d times", calls)
		}
	})
	t.Run("missing transport is unreachable", func(t *testing.T) {
		var nilReader *PeerObservabilityReader
		for name, reader := range map[string]*PeerObservabilityReader{
			"nil reader": nilReader, "nil transport": NewPeerObservabilityReader(nil),
		} {
			_, nodeErr := reader.QueryNodeSnapshot(context.Background(), "2")
			_, metricsErr := reader.QueryMetricsSnapshot(context.Background(), "2")
			for _, err := range []error{nodeErr, metricsErr} {
				if err == nil {
					t.Fatalf("%s: query succeeded", name)
				}
				requirePeerQueryCode(t, err, observationports.QueryErrorNodeUnreachable)
			}
		}
	})
	t.Run("closed transport is unreachable", func(t *testing.T) {
		if err := fixture.client.Close(); err != nil {
			t.Fatalf("close querying transport: %v", err)
		}
		_, err := fixture.reader.QueryNodeSnapshot(context.Background(), "2")
		requirePeerQueryCode(t, err, observationports.QueryErrorNodeUnreachable)
	})
}

func TestPeerObservabilityReaderMapsPeerStatusToQueryCodes(t *testing.T) {
	for _, test := range []struct {
		status PeerObservabilityStatus
		want   observationports.QueryErrorCode
	}{
		{PeerObservabilityStatusUnsupported, observationports.QueryErrorUnsupported},
		{PeerObservabilityStatusBusy, observationports.QueryErrorBusy},
		{PeerObservabilityStatusSnapshotTooLarge, observationports.QueryErrorSnapshotTooLarge},
		{PeerObservabilityStatusInternalError, observationports.QueryErrorFailed},
		{PeerObservabilityStatus(0), observationports.QueryErrorFailed},
		{PeerObservabilityStatus(99), observationports.QueryErrorFailed},
	} {
		err := peerObservabilityStatusError(test.status)
		requirePeerQueryCode(t, err, test.want)
	}

	fixture := newPeerReaderFixture(t)
	t.Run("local handler failure is a failed query", func(t *testing.T) {
		fixture.respond(func(context.Context, PeerObservabilityKind) ([]byte, error) { return nil, errors.New("backend down") })
		_, err := fixture.reader.QueryNodeSnapshot(context.Background(), "2")
		requirePeerQueryCode(t, err, observationports.QueryErrorFailed)
		_, err = fixture.reader.QueryMetricsSnapshot(context.Background(), "2")
		requirePeerQueryCode(t, err, observationports.QueryErrorFailed)
	})
	t.Run("oversized snapshot is reported as too large", func(t *testing.T) {
		fixture.respond(func(context.Context, PeerObservabilityKind) ([]byte, error) {
			return make([]byte, peerObservabilityResponseLimit), nil
		})
		_, err := fixture.reader.QueryNodeSnapshot(context.Background(), "2")
		requirePeerQueryCode(t, err, observationports.QueryErrorSnapshotTooLarge)
	})
	t.Run("peer without the capability is unsupported", func(t *testing.T) {
		fixture.client.mu.Lock()
		peer := fixture.client.peers[2]
		fixture.client.mu.Unlock()
		identity, _, _ := peer.identitySnapshot()
		identity.ObservabilitySupported = false
		if err := fixture.client.ReconcilePeers([]PeerIdentityDTO{identity}); err != nil {
			t.Fatalf("advertise peer without observability: %v", err)
		}
		before := fixture.calls.Load()
		_, err := fixture.reader.QueryNodeSnapshot(context.Background(), "2")
		requirePeerQueryCode(t, err, observationports.QueryErrorUnsupported)
		if fixture.calls.Load() != before {
			t.Fatal("an unsupported peer was still queried")
		}
	})
}

func TestPeerObservabilityBodyDecodingIsStrict(t *testing.T) {
	var target observationports.NodeDataDTO
	if err := decodePeerObservabilityBody([]byte(`{"nodeId":"2"}`+" \n"), &target); err != nil || target.NodeID != "2" {
		t.Fatalf("object with trailing whitespace = %+v, %v; want accepted", target, err)
	}
	for name, body := range map[string]string{
		"second JSON value": `{"nodeId":"2"} {"nodeId":"3"}`,
		"trailing garbage":  `{"nodeId":"2"} x`,
		"unknown field":     `{"nodeId":"2","extra":true}`,
		"truncated object":  `{"nodeId":"2"`,
		"not an object":     `[]`,
		"wrong field type":  `{"nodeId":2}`,
		"empty body":        ``,
	} {
		var decoded observationports.NodeDataDTO
		if err := decodePeerObservabilityBody([]byte(body), &decoded); err == nil {
			t.Errorf("%s: body was accepted as %+v", name, decoded)
		}
	}
	for _, body := range []string{`{"nodeId":"2"} {"nodeId":"3"}`, `{"nodeId":"2"} x`} {
		var decoded observationports.NodeDataDTO
		if err := decodePeerObservabilityBody([]byte(body), &decoded); !errors.Is(err, ErrInvalidPeerMessage) {
			t.Errorf("trailing content %q error = %v, want ErrInvalidPeerMessage", body, err)
		}
	}
}

func TestPeerObservabilityMemberIDMustBeCanonicalDecimal(t *testing.T) {
	for value, want := range map[string]uint64{"1": 1, "42": 42, "18446744073709551615": math.MaxUint64} {
		got, err := parseObservabilityMemberID(value)
		if err != nil || got != want {
			t.Errorf("parse %q = %d, %v; want %d", value, got, err, want)
		}
	}
	for _, value := range []string{"", "0", "00", "01", "+1", "-1", "1.0", "0x1", "1 ", "18446744073709551616", "abc"} {
		if id, err := parseObservabilityMemberID(value); !errors.Is(err, ErrInvalidPeerMessage) || id != 0 {
			t.Errorf("parse %q = %d, %v; want ErrInvalidPeerMessage", value, id, err)
		}
	}
}

func TestPeerMetricValuesMustRoundTripCanonically(t *testing.T) {
	integer := func(value uint64) metricsports.MetricValueDTO {
		return metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: value}
	}
	floating := func(value float64) metricsports.MetricValueDTO {
		return metricsports.MetricValueDTO{Kind: metricsports.MetricValueFloat, Float64: value}
	}
	for _, test := range []struct {
		kind, value string
		want        metricsports.MetricValueDTO
	}{
		{"integer", "0", integer(0)},
		{"integer", strconv.FormatUint(math.MaxUint64, 10), integer(math.MaxUint64)},
		{"float", "0.25", floating(0.25)},
		{"float", "1e+06", floating(1e6)},
		{"float", "-3.5", floating(-3.5)},
	} {
		got, err := peerMetricValue(test.kind, test.value)
		if err != nil || got != test.want {
			t.Errorf("%s %q = %+v, %v; want %+v", test.kind, test.value, got, err, test.want)
		}
	}
	for _, test := range []struct{ kind, value string }{
		{"integer", ""}, {"integer", "-1"}, {"integer", "+1"}, {"integer", "01"}, {"integer", "1.0"},
		{"integer", "18446744073709551616"},
		{"float", ""}, {"float", "abc"}, {"float", "NaN"}, {"float", "Inf"}, {"float", "+Inf"}, {"float", "-Inf"},
		{"float", "0.250"}, {"float", "1.0"}, {"float", "1e6"}, {"float", "1e999"},
		{"string", "1"}, {"", "1"}, {"INTEGER", "1"},
	} {
		got, err := peerMetricValue(test.kind, test.value)
		if !errors.Is(err, ErrInvalidPeerMessage) || got != (metricsports.MetricValueDTO{}) {
			t.Errorf("%s %q = %+v, %v; want ErrInvalidPeerMessage and no value", test.kind, test.value, got, err)
		}
	}
}
