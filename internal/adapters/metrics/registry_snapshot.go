package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	ports "narumq/internal/application/ports/metrics"
)

var errNilMetricVisitor = errors.New("metric visitor is nil")
var errNilMetricRegistry = errors.New("metrics registry is nil")

var _ ports.MetricsSnapshotSource = (*Registry)(nil)

// VisitMetrics visits a live registry snapshot without building a complete metric tree.
// Visitors must consume each sample before the next call. They must copy descriptor or
// sample slices that they retain.
func (r *Registry) VisitMetrics(visitor ports.MetricVisitor) error {
	if r == nil {
		return errNilMetricRegistry
	}
	if visitor == nil {
		return errNilMetricVisitor
	}

	r.mu.RLock()
	subscriptionCounter := r.subscriptionCounter
	r.mu.RUnlock()
	subscriptionCount := 0
	if subscriptionCounter != nil {
		subscriptionCount = subscriptionCounter.SubscriptionCount()
	}

	emitter := metricVisitorEmitter{visitor: visitor}
	for _, descriptor := range r.metricCatalog {
		if descriptor.Name == "narumq_storage_free_bytes" && !r.storageFreeBytesSet.Load() {
			continue
		}
		if err := visitor.BeginFamily(descriptor); err != nil {
			return err
		}

		var err error
		switch descriptor.Name {
		case "narumq_audit_events_total":
			for index, outcome := range auditOutcomes {
				if err = emitter.sample1(descriptor.Name, "outcome", outcome, uintValue(uint64(r.auditOutcomeCounts[index].Load()))); err != nil {
					break
				}
			}
		case "narumq_audit_publisher_ready":
			err = emitter.sample(descriptor.Name, boolValue(r.auditPublisherReady.Load()))
		case "narumq_auth_attempts_total":
			for _, entry := range r.authAttemptCounters() {
				if err = emitter.sample3(descriptor.Name, "method", entry.key.method, "result", entry.key.result, "reason", entry.key.reason, uintValue(entry.value)); err != nil {
					break
				}
			}
		case "narumq_cluster_commit_backlog_oldest_seconds":
			err = emitter.sample(descriptor.Name, floatValue(float64(r.commitBacklogOldest.Load())/float64(time.Second)))
		case "narumq_cluster_policy_committed":
			err = emitter.sample(descriptor.Name, uintValue(uint64(r.clusterPolicyCommitted.Load())))
		case "narumq_cluster_policy_config_mismatch":
			err = emitter.sample(descriptor.Name, uintValue(uint64(r.clusterPolicyConfigMismatch.Load())))
		case "narumq_cluster_policy_version":
			err = emitter.sample(descriptor.Name, uintValue(r.clusterPolicyVersion.Load()))
		case "narumq_connections_active":
			err = emitter.sample(descriptor.Name, uintValue(uint64(r.ActiveConnections())))
		case "narumq_connections_total":
			for _, entry := range r.connectionCounters() {
				if err = emitter.sample3(descriptor.Name, "protocol", entry.key.protocol, "transport", entry.key.transport, "result", entry.key.result, uintValue(entry.value)); err != nil {
					break
				}
			}
		case "narumq_durable_delivery_saturated_targets_total":
			err = emitter.sample(descriptor.Name, uintValue(r.durableDeliverySaturated.Load()))
		case "narumq_messages_dropped_total":
			for index, reason := range messageDropReasons {
				if err = emitter.sample1(descriptor.Name, "reason", reason, uintValue(r.messageDrops[index].Load())); err != nil {
					break
				}
			}
		case "narumq_mqtt_connect_rejected_total":
			err = emitter.sample2(descriptor.Name, "protocol", ports.ProtocolMQTT310, "reason", ports.ConnectRejectReasonUnsupportedProtocolVersion, uintValue(uint64(r.connectRejectedTotal.Load())))
			for protocolIndex, protocol := range connectRejectedProtocols {
				for reasonIndex, reason := range connectRejectReasons {
					if err != nil {
						break
					}
					err = emitter.sample2(descriptor.Name, "protocol", protocol, "reason", reason, uintValue(r.connectRejectedByProtocol[protocolIndex][reasonIndex].Load()))
				}
			}
		case "narumq_mqtt_listener_admission_rejected_total":
			for transportIndex, transport := range listenerAdmissionTransports {
				for reasonIndex, reason := range listenerAdmissionReasons {
					if err != nil {
						break
					}
					err = emitter.sample2(descriptor.Name, "transport", transport, "reason", reason, uintValue(uint64(r.listenerAdmissionRejections[transportIndex][reasonIndex].Load())))
				}
			}
		case "narumq_node_drain_active":
			err = emitter.sample(descriptor.Name, boolValue(r.nodeDrainActive.Load()))
		case "narumq_node_drain_progress":
			for index, quantity := range nodeDrainProgressQuantities {
				if err = emitter.sample1(descriptor.Name, "quantity", quantity, uintValue(r.nodeDrainProgress[index].Load())); err != nil {
					break
				}
			}
		case "narumq_node_drain_transitions_total":
			for _, entry := range r.nodeDrainTransitionCounters() {
				if err = emitter.sample1(descriptor.Name, "phase", entry.key.phase, uintValue(entry.value)); err != nil {
					break
				}
			}
		case "narumq_packets_received_total":
			for _, entry := range r.packetReceivedCounters() {
				if err = emitter.sample1(descriptor.Name, "packet_type", entry.key.packetType, uintValue(entry.value)); err != nil {
					break
				}
			}
		case "narumq_packets_sent_total":
			for _, entry := range r.packetSentCounters() {
				if err = emitter.sample1(descriptor.Name, "packet_type", entry.key.packetType, uintValue(entry.value)); err != nil {
					break
				}
			}
		case "narumq_publish_backpressure_active":
			for index, reason := range publishBackpressureReasons[:len(r.publishBackpressureActive)] {
				if err = emitter.sample1(descriptor.Name, "reason", reason, boolValue(r.publishBackpressureActive[index].Load())); err != nil {
					break
				}
			}
		case "narumq_publish_backpressure_rejections_total":
			for index, reason := range publishBackpressureReasons {
				if err = emitter.sample1(descriptor.Name, "reason", reason, uintValue(r.publishBackpressureRejections[index].Load())); err != nil {
					break
				}
			}
		case "narumq_publish_latency_seconds":
			err = emitter.histogram(descriptor.Name, r.publishLatency, "", "")
		case "narumq_raft_peer_replication_lag_entries":
			for _, entry := range r.peerReplicationLagSnapshot() {
				if err = emitter.sample1(descriptor.Name, "peer", strconv.FormatUint(entry.peer, 10), uintValue(entry.lag)); err != nil {
					break
				}
			}
		case "narumq_raft_stage_duration_seconds":
			for index, histogram := range r.raftStages {
				if err = emitter.histogram(descriptor.Name, histogram, "stage", raftStageNames[index]); err != nil {
					break
				}
			}
		case "narumq_raft_wal_sync_duration_seconds":
			err = emitter.histogram(descriptor.Name, r.walSyncDuration, "", "")
		case "narumq_raft_wal_sync_entries":
			err = emitter.histogram(descriptor.Name, r.walSyncEntries, "", "")
		case "narumq_raft_wal_written_bytes_total":
			err = emitter.sample(descriptor.Name, uintValue(r.walWrittenBytes.Load()))
		case "narumq_retained_bytes":
			err = emitter.sample(descriptor.Name, uintValue(uint64(r.retainedBytes.Load())))
		case "narumq_retained_messages":
			err = emitter.sample(descriptor.Name, uintValue(uint64(r.retainedMessages.Load())))
		case "narumq_sessions_active":
			if err = emitter.sample1(descriptor.Name, "persistent", "true", uintValue(uint64(r.sessionsActivePersistent.Load()))); err == nil {
				err = emitter.sample1(descriptor.Name, "persistent", "false", uintValue(uint64(r.sessionsActiveNonPersistent.Load())))
			}
		case "narumq_storage_free_bytes":
			err = emitter.sample(descriptor.Name, uintValue(r.storageFreeBytes.Load()))
		case "narumq_subscriptions_active":
			err = emitter.sample(descriptor.Name, uintValue(uint64(subscriptionCount)))
		case "narumq_tenant_limit_rejections_total":
			err = emitter.tenantRejections(descriptor.Name, r.tenantLimitRejectionCounters())
		case "narumq_tenant_quota_config_mismatch":
			err = emitter.sample(descriptor.Name, uintValue(uint64(r.tenantQuotaConfigMismatch.Load())))
		case "narumq_tenant_quota_profile_committed":
			err = emitter.sample(descriptor.Name, uintValue(uint64(r.tenantQuotaProfileCommitted.Load())))
		case "narumq_tenant_quota_profile_version":
			err = emitter.sample(descriptor.Name, uintValue(r.tenantQuotaProfileVersion.Load()))
		case "narumq_tenant_quota_rejections_total":
			err = emitter.tenantRejections(descriptor.Name, r.tenantQuotaRejectionCounters())
		case "narumq_will_outcomes_total":
			for index, reason := range willOutcomeReasons {
				if err = emitter.sample1(descriptor.Name, "reason", reason, uintValue(uint64(r.willOutcomes[index].Load()))); err != nil {
					break
				}
			}
		default:
			err = fmt.Errorf("metric catalog contains unsupported family %q", descriptor.Name)
		}
		if err != nil {
			return err
		}
		if err := visitor.EndFamily(); err != nil {
			return err
		}
	}
	return nil
}

// CaptureMetrics returns an owned snapshot with deterministic family and label ordering.
func (r *Registry) CaptureMetrics() (ports.MetricsSnapshotDTO, error) {
	if r == nil {
		return ports.MetricsSnapshotDTO{}, errNilMetricRegistry
	}
	visitor := metricsSnapshotVisitor{snapshot: ports.MetricsSnapshotDTO{Families: make([]ports.MetricFamilyDTO, 0, len(r.metricCatalog))}}
	if err := r.VisitMetrics(&visitor); err != nil {
		return ports.MetricsSnapshotDTO{}, err
	}
	return visitor.snapshot, nil
}

// StartedAt returns the UTC time when this Registry instance was created.
func (r *Registry) StartedAt() time.Time {
	return r.startedAt
}

type metricVisitorEmitter struct {
	visitor ports.MetricVisitor
	labels  [3]ports.MetricLabelDTO
}

func (e *metricVisitorEmitter) sample(name string, value ports.MetricValueDTO) error {
	return e.visitor.Sample(ports.MetricSampleDTO{Name: name, Value: value})
}

func (e *metricVisitorEmitter) sample1(name, labelName, labelValue string, value ports.MetricValueDTO) error {
	e.labels[0] = ports.MetricLabelDTO{Name: labelName, Value: labelValue}
	return e.visitor.Sample(ports.MetricSampleDTO{Name: name, Labels: e.labels[:1], Value: value})
}

func (e *metricVisitorEmitter) sample2(name, firstName, firstValue, secondName, secondValue string, value ports.MetricValueDTO) error {
	e.labels[0] = ports.MetricLabelDTO{Name: firstName, Value: firstValue}
	e.labels[1] = ports.MetricLabelDTO{Name: secondName, Value: secondValue}
	return e.visitor.Sample(ports.MetricSampleDTO{Name: name, Labels: e.labels[:2], Value: value})
}

func (e *metricVisitorEmitter) sample3(name, firstName, firstValue, secondName, secondValue, thirdName, thirdValue string, value ports.MetricValueDTO) error {
	e.labels[0] = ports.MetricLabelDTO{Name: firstName, Value: firstValue}
	e.labels[1] = ports.MetricLabelDTO{Name: secondName, Value: secondValue}
	e.labels[2] = ports.MetricLabelDTO{Name: thirdName, Value: thirdValue}
	return e.visitor.Sample(ports.MetricSampleDTO{Name: name, Labels: e.labels[:3], Value: value})
}

func (e *metricVisitorEmitter) tenantRejections(name string, entries []tenantQuotaMetricEntry) error {
	for _, entry := range entries {
		if err := e.sample2(name, "tenant", entry.tenantID, "dimension", entry.dimension, uintValue(uint64(entry.count))); err != nil {
			return err
		}
	}
	return nil
}

func (e *metricVisitorEmitter) histogram(name string, histogram *Histogram, labelName, labelValue string) error {
	count := histogram.count.Load()
	for index, bound := range histogram.buckets {
		le := strconv.FormatFloat(bound, 'g', -1, 64)
		var err error
		if labelName == "" {
			err = e.sample1(name+"_bucket", "le", le, uintValue(histogram.counts[index].Load()))
		} else {
			err = e.sample2(name+"_bucket", labelName, labelValue, "le", le, uintValue(histogram.counts[index].Load()))
		}
		if err != nil {
			return err
		}
	}
	var err error
	if labelName == "" {
		err = e.sample1(name+"_bucket", "le", "+Inf", uintValue(count))
	} else {
		err = e.sample2(name+"_bucket", labelName, labelValue, "le", "+Inf", uintValue(count))
	}
	if err != nil {
		return err
	}
	sum := math.Float64frombits(histogram.sumBits.Load())
	if labelName == "" {
		if err = e.sample(name+"_sum", floatValue(sum)); err == nil {
			err = e.sample(name+"_count", uintValue(count))
		}
	} else {
		if err = e.sample1(name+"_sum", labelName, labelValue, floatValue(sum)); err == nil {
			err = e.sample1(name+"_count", labelName, labelValue, uintValue(count))
		}
	}
	return err
}

type metricsSnapshotVisitor struct {
	snapshot ports.MetricsSnapshotDTO
	current  int
}

func (v *metricsSnapshotVisitor) BeginFamily(descriptor ports.MetricDescriptorDTO) error {
	descriptor.LabelNames = append([]string(nil), descriptor.LabelNames...)
	v.snapshot.Families = append(v.snapshot.Families, ports.MetricFamilyDTO{Descriptor: descriptor})
	v.current = len(v.snapshot.Families) - 1
	return nil
}

func (v *metricsSnapshotVisitor) Sample(sample ports.MetricSampleDTO) error {
	sample.Labels = append([]ports.MetricLabelDTO(nil), sample.Labels...)
	sort.Slice(sample.Labels, func(left, right int) bool { return sample.Labels[left].Name < sample.Labels[right].Name })
	family := &v.snapshot.Families[v.current]
	family.Samples = append(family.Samples, sample)
	return nil
}

func (*metricsSnapshotVisitor) EndFamily() error { return nil }

type textMetricVisitor struct {
	buffer *bytes.Buffer
}

func (v *textMetricVisitor) BeginFamily(descriptor ports.MetricDescriptorDTO) error {
	fmt.Fprintf(v.buffer, "# HELP %s %s\n", descriptor.Name, descriptor.Help)
	fmt.Fprintf(v.buffer, "# TYPE %s %s\n", descriptor.Name, descriptor.Type)
	return nil
}

func (v *textMetricVisitor) Sample(sample ports.MetricSampleDTO) error {
	v.buffer.WriteString(sample.Name)
	if len(sample.Labels) > 0 {
		v.buffer.WriteByte('{')
		for index, label := range sample.Labels {
			if index > 0 {
				v.buffer.WriteByte(',')
			}
			v.buffer.WriteString(label.Name)
			v.buffer.WriteString("=\"")
			v.buffer.WriteString(prometheusLabelEscaper.Replace(label.Value))
			v.buffer.WriteByte('"')
		}
		v.buffer.WriteByte('}')
	}
	v.buffer.WriteByte(' ')
	if sample.Value.Kind == ports.MetricValueFloat {
		fmt.Fprintf(v.buffer, "%.6f", sample.Value.Float64)
	} else {
		fmt.Fprintf(v.buffer, "%d", sample.Value.Uint64)
	}
	v.buffer.WriteByte('\n')
	return nil
}

func (*textMetricVisitor) EndFamily() error { return nil }

func uintValue(value uint64) ports.MetricValueDTO {
	return ports.MetricValueDTO{Kind: ports.MetricValueInteger, Uint64: value}
}

func floatValue(value float64) ports.MetricValueDTO {
	return ports.MetricValueDTO{Kind: ports.MetricValueFloat, Float64: value}
}

func boolValue(value bool) ports.MetricValueDTO {
	if value {
		return uintValue(1)
	}
	return uintValue(0)
}

type connectionCounterSnapshot struct {
	key   connectionMetricKey
	value uint64
}

type packetCounterSnapshot struct {
	key   packetMetricKey
	value uint64
}

type authAttemptCounterSnapshot struct {
	key   authAttemptMetricKey
	value uint64
}

type nodeDrainCounterSnapshot struct {
	key   nodeDrainMetricKey
	value uint64
}

func (r *Registry) connectionCounters() []connectionCounterSnapshot {
	r.mu.RLock()
	entries := make([]connectionCounterSnapshot, 0, len(r.connectionsTotal))
	for key, counter := range r.connectionsTotal {
		entries = append(entries, connectionCounterSnapshot{key: key, value: uint64(counter.Load())})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool {
		l, rr := entries[left].key, entries[right].key
		if l.protocol != rr.protocol {
			return l.protocol < rr.protocol
		}
		if l.transport != rr.transport {
			return l.transport < rr.transport
		}
		return l.result < rr.result
	})
	return entries
}

func (r *Registry) packetReceivedCounters() []packetCounterSnapshot {
	r.mu.RLock()
	entries := make([]packetCounterSnapshot, 0, len(r.packetsReceivedTotal))
	for key, counter := range r.packetsReceivedTotal {
		entries = append(entries, packetCounterSnapshot{key: key, value: uint64(counter.Load())})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool { return entries[left].key.packetType < entries[right].key.packetType })
	return entries
}

func (r *Registry) packetSentCounters() []packetCounterSnapshot {
	r.mu.RLock()
	entries := make([]packetCounterSnapshot, 0, len(r.packetsSentTotal))
	for key, counter := range r.packetsSentTotal {
		entries = append(entries, packetCounterSnapshot{key: key, value: uint64(counter.Load())})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool { return entries[left].key.packetType < entries[right].key.packetType })
	return entries
}

func (r *Registry) authAttemptCounters() []authAttemptCounterSnapshot {
	r.mu.RLock()
	entries := make([]authAttemptCounterSnapshot, 0, len(r.authAttemptsTotal))
	for key, counter := range r.authAttemptsTotal {
		entries = append(entries, authAttemptCounterSnapshot{key: key, value: uint64(counter.Load())})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool {
		l, rr := entries[left].key, entries[right].key
		if l.method != rr.method {
			return l.method < rr.method
		}
		if l.result != rr.result {
			return l.result < rr.result
		}
		return l.reason < rr.reason
	})
	return entries
}

func (r *Registry) nodeDrainTransitionCounters() []nodeDrainCounterSnapshot {
	r.mu.RLock()
	entries := make([]nodeDrainCounterSnapshot, 0, len(r.nodeDrainTransitions))
	for key, counter := range r.nodeDrainTransitions {
		entries = append(entries, nodeDrainCounterSnapshot{key: key, value: uint64(counter.Load())})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool { return entries[left].key.phase < entries[right].key.phase })
	return entries
}

func (r *Registry) tenantQuotaRejectionCounters() []tenantQuotaMetricEntry {
	return r.tenantRejectionCounters(r.tenantQuotaRejections)
}

func (r *Registry) tenantLimitRejectionCounters() []tenantQuotaMetricEntry {
	return r.tenantRejectionCounters(r.tenantLimitRejections)
}

func (r *Registry) tenantRejectionCounters(counters map[tenantQuotaMetricKey]*atomic.Int64) []tenantQuotaMetricEntry {
	r.mu.RLock()
	entries := make([]tenantQuotaMetricEntry, 0, len(counters))
	for key, counter := range counters {
		entries = append(entries, tenantQuotaMetricEntry{tenantID: key.tenantID, dimension: key.dimension, count: counter.Load()})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].tenantID != entries[right].tenantID {
			return entries[left].tenantID < entries[right].tenantID
		}
		return entries[left].dimension < entries[right].dimension
	})
	return entries
}

func (r *Registry) peerReplicationLagSnapshot() []peerReplicationLagEntry {
	r.mu.RLock()
	entries := append([]peerReplicationLagEntry(nil), r.peerReplicationLag...)
	r.mu.RUnlock()
	return entries
}
