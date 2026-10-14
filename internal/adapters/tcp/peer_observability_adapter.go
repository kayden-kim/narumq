package tcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"

	metricsports "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
)

// PeerObservabilityReader converts the authenticated wire response to application snapshots.
type PeerObservabilityReader struct {
	transport *DynamicTCPPeerTransport
}

func NewPeerObservabilityReader(transport *DynamicTCPPeerTransport) *PeerObservabilityReader {
	return &PeerObservabilityReader{transport: transport}
}

func (reader *PeerObservabilityReader) QueryNodeSnapshot(ctx context.Context, memberID string) (observationports.NodeSnapshotDTO, error) {
	peerID, err := parseObservabilityMemberID(memberID)
	if err != nil {
		return observationports.NodeSnapshotDTO{}, peerObservabilityError(observationports.QueryErrorInvalidResponse)
	}
	response, err := reader.query(ctx, peerID, PeerObservabilityNode)
	if err != nil {
		return observationports.NodeSnapshotDTO{}, err
	}
	if response.Status != PeerObservabilityStatusOK {
		return observationports.NodeSnapshotDTO{}, peerObservabilityStatusError(response.Status)
	}
	var data observationports.NodeDataDTO
	if err := decodePeerObservabilityBody(response.Body, &data); err != nil || data.NodeID != memberID ||
		data.QueryStatus != observationports.QueryStatusOK || data.QueryErrorCode != nil || data.ObservedAt == nil {
		return observationports.NodeSnapshotDTO{}, peerObservabilityError(observationports.QueryErrorInvalidResponse)
	}
	return observationports.NodeSnapshotDTO{
		ObservedAt: *data.ObservedAt, Version: data.Version, UptimeMillis: data.UptimeMillis,
		State: data.State, Ready: data.Ready, ReadinessReason: data.ReadinessReason, Role: data.Role,
		LastAppliedIndex: data.LastAppliedIndex, ConnectionCount: data.ConnectionCount,
		SessionCount: data.SessionCount, StoragePressure: data.StoragePressure,
		ReplicationHealth: data.ReplicationHealth, LastErrorCode: data.LastErrorCode,
	}, nil
}

func (reader *PeerObservabilityReader) QueryMetricsSnapshot(ctx context.Context, memberID string) (observationports.MetricsSnapshotDTO, error) {
	peerID, err := parseObservabilityMemberID(memberID)
	if err != nil {
		return observationports.MetricsSnapshotDTO{}, peerObservabilityError(observationports.QueryErrorInvalidResponse)
	}
	response, err := reader.query(ctx, peerID, PeerObservabilityMetrics)
	if err != nil {
		return observationports.MetricsSnapshotDTO{}, err
	}
	if response.Status != PeerObservabilityStatusOK {
		return observationports.MetricsSnapshotDTO{}, peerObservabilityStatusError(response.Status)
	}
	var data observationports.NodeMetricsDataDTO
	if err := decodePeerObservabilityBody(response.Body, &data); err != nil || data.NodeID != memberID ||
		data.QueryStatus != observationports.QueryStatusOK || data.QueryErrorCode != nil || data.ObservedAt == nil ||
		data.StartedAt == nil || data.Families == nil {
		return observationports.MetricsSnapshotDTO{}, peerObservabilityError(observationports.QueryErrorInvalidResponse)
	}
	snapshot := metricsports.MetricsSnapshotDTO{Families: make([]metricsports.MetricFamilyDTO, 0, len(data.Families))}
	for _, family := range data.Families {
		converted := metricsports.MetricFamilyDTO{
			Descriptor: metricsports.MetricDescriptorDTO{
				Name: family.Name, Help: family.Help, Type: metricsports.MetricType(family.Type),
				Scope: metricsports.MetricScope(family.Scope), Aggregation: metricsports.MetricAggregation(family.Aggregation),
				LabelNames: append([]string(nil), family.LabelNames...),
			},
			Samples: make([]metricsports.MetricSampleDTO, 0, len(family.Samples)),
		}
		for _, sample := range family.Samples {
			value, err := peerMetricValue(sample.ValueKind, sample.Value)
			if err != nil {
				return observationports.MetricsSnapshotDTO{}, peerObservabilityError(observationports.QueryErrorInvalidResponse)
			}
			labels := make([]metricsports.MetricLabelDTO, 0, len(sample.Labels))
			for _, label := range sample.Labels {
				labels = append(labels, metricsports.MetricLabelDTO{Name: label.Name, Value: label.Value})
			}
			converted.Samples = append(converted.Samples, metricsports.MetricSampleDTO{
				Name: sample.Name, Labels: labels, Value: value,
			})
		}
		snapshot.Families = append(snapshot.Families, converted)
	}
	return observationports.MetricsSnapshotDTO{
		ObservedAt: *data.ObservedAt, StartedAt: *data.StartedAt, Snapshot: snapshot,
	}, nil
}

func (reader *PeerObservabilityReader) query(ctx context.Context, peerID uint64, kind PeerObservabilityKind) (PeerObservabilityResponseDTO, error) {
	if reader == nil || reader.transport == nil {
		return PeerObservabilityResponseDTO{}, peerObservabilityError(observationports.QueryErrorNodeUnreachable)
	}
	response, err := reader.transport.QueryObservability(ctx, peerID, kind)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return PeerObservabilityResponseDTO{}, peerObservabilityError(observationports.QueryErrorTimeout)
		}
		if errors.Is(err, ErrInvalidPeerMessage) {
			return PeerObservabilityResponseDTO{}, peerObservabilityError(observationports.QueryErrorInvalidResponse)
		}
		return PeerObservabilityResponseDTO{}, peerObservabilityError(observationports.QueryErrorNodeUnreachable)
	}
	return response, nil
}

// NewPeerObservabilityHandler exposes only the local query-service snapshots over peer TLS.
func NewPeerObservabilityHandler(service observationports.QueryService, localNodeID string) PeerObservabilityHandler {
	return func(ctx context.Context, kind PeerObservabilityKind) ([]byte, error) {
		if service == nil || localNodeID == "" {
			return nil, errors.New("observability query service is unavailable")
		}
		switch kind {
		case PeerObservabilityNode:
			data, err := service.GetNode(ctx, localNodeID)
			if err != nil {
				return nil, err
			}
			if data.QueryStatus != observationports.QueryStatusOK {
				return nil, errors.New("local node snapshot is unavailable")
			}
			return json.Marshal(data)
		case PeerObservabilityMetrics:
			data, err := service.GetNodeMetrics(ctx, localNodeID)
			if err != nil {
				return nil, err
			}
			if data.QueryStatus != observationports.QueryStatusOK {
				return nil, errors.New("local node metrics are unavailable")
			}
			return json.Marshal(data)
		default:
			return nil, ErrInvalidPeerMessage
		}
	}
}

func parseObservabilityMemberID(value string) (uint64, error) {
	memberID, err := strconv.ParseUint(value, 10, 64)
	if err != nil || memberID == 0 || strconv.FormatUint(memberID, 10) != value {
		return 0, ErrInvalidPeerMessage
	}
	return memberID, nil
}

func decodePeerObservabilityBody(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrInvalidPeerMessage
	}
	return nil
}

func peerMetricValue(kind, value string) (metricsports.MetricValueDTO, error) {
	switch metricsports.MetricValueKind(kind) {
	case metricsports.MetricValueInteger:
		integer, err := strconv.ParseUint(value, 10, 64)
		if err != nil || strconv.FormatUint(integer, 10) != value {
			return metricsports.MetricValueDTO{}, ErrInvalidPeerMessage
		}
		return metricsports.MetricValueDTO{Kind: metricsports.MetricValueInteger, Uint64: integer}, nil
	case metricsports.MetricValueFloat:
		floating, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(floating) || math.IsInf(floating, 0) ||
			strconv.FormatFloat(floating, 'g', -1, 64) != value {
			return metricsports.MetricValueDTO{}, ErrInvalidPeerMessage
		}
		return metricsports.MetricValueDTO{Kind: metricsports.MetricValueFloat, Float64: floating}, nil
	default:
		return metricsports.MetricValueDTO{}, ErrInvalidPeerMessage
	}
}

func peerObservabilityStatusError(status PeerObservabilityStatus) error {
	switch status {
	case PeerObservabilityStatusUnsupported:
		return peerObservabilityError(observationports.QueryErrorUnsupported)
	case PeerObservabilityStatusBusy:
		return peerObservabilityError(observationports.QueryErrorBusy)
	case PeerObservabilityStatusSnapshotTooLarge:
		return peerObservabilityError(observationports.QueryErrorSnapshotTooLarge)
	default:
		return peerObservabilityError(observationports.QueryErrorFailed)
	}
}

func peerObservabilityError(code observationports.QueryErrorCode) error {
	return &observationports.PeerQueryError{Code: code}
}

var _ observationports.PeerSnapshotReader = (*PeerObservabilityReader)(nil)
