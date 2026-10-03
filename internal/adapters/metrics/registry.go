package metrics

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	ports "narumq/internal/application/ports/metrics"
	"narumq/internal/domain/tenancy"
)

const (
	maxTenantQuotaMetricTenants = 1024
	tenantQuotaOverflowLabel    = "_overflow"
)

type tenantQuotaMetricKey struct {
	tenantID  string
	dimension string
}

type tenantQuotaMetricEntry struct {
	tenantID  string
	dimension string
	count     int64
}

var prometheusLabelEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")

// Default histogram buckets for publish latency in seconds.
var defaultPublishLatencyBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
}

// Default histogram buckets for Raft WAL sync observations.
var defaultWALSyncDurationBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}

var defaultWALSyncEntriesBuckets = []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256}

// Histogram tracks distribution of floating-point observations.
type Histogram struct {
	buckets []float64
	counts  []atomic.Uint64
	count   atomic.Uint64
	sumBits atomic.Uint64 // float64 bits
}

// NewHistogram creates a thread-safe histogram with given upper bounds.
func NewHistogram(buckets []float64) *Histogram {
	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	sort.Float64s(sorted)

	return &Histogram{
		buckets: sorted,
		counts:  make([]atomic.Uint64, len(sorted)),
	}
}

// Observe adds an observation to the histogram.
func (h *Histogram) Observe(val float64) {
	if val < 0 {
		val = 0
	}
	for i, b := range h.buckets {
		if val <= b {
			h.counts[i].Add(1)
		}
	}
	h.count.Add(1)

	// atomic float add via CAS
	for {
		oldBits := h.sumBits.Load()
		newSum := math.Float64frombits(oldBits) + val
		newBits := math.Float64bits(newSum)
		if h.sumBits.CompareAndSwap(oldBits, newBits) {
			break
		}
	}
}

// Snapshot returns the current state of the histogram.
func (h *Histogram) Snapshot() (buckets []float64, counts []uint64, count uint64, sum float64) {
	buckets = make([]float64, len(h.buckets))
	copy(buckets, h.buckets)

	counts = make([]uint64, len(h.counts))
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
	}
	count = h.count.Load()
	sum = math.Float64frombits(h.sumBits.Load())
	return
}

// Registry stores operational metrics and renders them in Prometheus / OpenMetrics text formats.
type Registry struct {
	mu sync.RWMutex

	connectionsActive atomic.Int64
	connectionsTotal  map[string]*atomic.Int64 // key: protocol,transport,result

	sessionsActivePersistent    atomic.Int64
	sessionsActiveNonPersistent atomic.Int64

	subscriptionsActive atomic.Int64
	retainedMessages    atomic.Int64
	retainedBytes       atomic.Int64

	packetsReceivedTotal        map[string]*atomic.Int64 // key: packetType
	packetsSentTotal            map[string]*atomic.Int64 // key: packetType
	authAttemptsTotal           map[string]*atomic.Int64 // key: method,result,reason
	connectRejectedTotal        atomic.Int64
	nodeDrainTransitions        map[string]*atomic.Int64
	tenantQuotaRejections       map[tenantQuotaMetricKey]*atomic.Int64
	tenantQuotaTenants          map[string]struct{}
	tenantQuotaProfileCommitted atomic.Int64
	tenantQuotaProfileVersion   atomic.Uint64
	tenantQuotaConfigMismatch   atomic.Int64

	publishLatency  *Histogram
	walSyncDuration *Histogram
	walSyncEntries  *Histogram
	walWrittenBytes atomic.Uint64
	raftStages      [raftStageCount]*Histogram
}

// NewRegistry initializes an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		connectionsTotal:      make(map[string]*atomic.Int64),
		packetsReceivedTotal:  make(map[string]*atomic.Int64),
		packetsSentTotal:      make(map[string]*atomic.Int64),
		authAttemptsTotal:     make(map[string]*atomic.Int64),
		nodeDrainTransitions:  make(map[string]*atomic.Int64),
		tenantQuotaRejections: make(map[tenantQuotaMetricKey]*atomic.Int64),
		tenantQuotaTenants:    make(map[string]struct{}),
		publishLatency:        NewHistogram(defaultPublishLatencyBuckets),
		walSyncDuration:       NewHistogram(defaultWALSyncDurationBuckets),
		walSyncEntries:        NewHistogram(defaultWALSyncEntriesBuckets),
		raftStages:            newRaftStageHistograms(),
	}
}

var _ ports.MetricsRecorder = (*Registry)(nil)

func (r *Registry) getCounter(m map[string]*atomic.Int64, key string) *atomic.Int64 {
	r.mu.RLock()
	c, ok := m[key]
	r.mu.RUnlock()
	if ok {
		return c
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok = m[key]; ok {
		return c
	}
	c = &atomic.Int64{}
	m[key] = c
	return c
}

// ConnectionOpened increments active connections and total counter.
func (r *Registry) ConnectionOpened(protocol string, transport string) {
	protocol = normalizeProtocol(protocol)
	transport = normalizeTransport(transport)
	r.connectionsActive.Add(1)
	key := fmt.Sprintf(`protocol="%s",transport="%s",result="success"`, protocol, transport)
	r.getCounter(r.connectionsTotal, key).Add(1)
}

// ConnectionClosed decrements active connections and records a closed connection.
func (r *Registry) ConnectionClosed(protocol string, transport string) {
	protocol = normalizeProtocol(protocol)
	transport = normalizeTransport(transport)
	for {
		current := r.connectionsActive.Load()
		if current <= 0 || r.connectionsActive.CompareAndSwap(current, current-1) {
			break
		}
	}
	key := fmt.Sprintf(`protocol="%s",transport="%s",result="closed"`, protocol, transport)
	r.getCounter(r.connectionsTotal, key).Add(1)
}

func normalizeProtocol(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case ports.ProtocolMQTT311, "MQTT311":
		return ports.ProtocolMQTT311
	case ports.ProtocolMQTT50, "MQTT50":
		return ports.ProtocolMQTT50
	default:
		return "UNKNOWN"
	}
}

func normalizeTransport(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case ports.TransportTCP:
		return ports.TransportTCP
	case ports.TransportTLS:
		return ports.TransportTLS
	case ports.TransportWS:
		return ports.TransportWS
	case ports.TransportWSS:
		return ports.TransportWSS
	default:
		return "unknown"
	}
}

func normalizePacketType(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	switch value {
	case "CONNECT", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL", "PUBCOMP",
		"SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", "PINGREQ", "PINGRESP", "DISCONNECT":
		return value
	default:
		return "UNKNOWN"
	}
}

func normalizeAuthMethod(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case ports.AuthMethodAnonymous:
		return ports.AuthMethodAnonymous
	case ports.AuthMethodPassword:
		return ports.AuthMethodPassword
	case ports.AuthMethodTLS:
		return ports.AuthMethodTLS
	case ports.AuthMethodJWT:
		return ports.AuthMethodJWT
	default:
		return "other"
	}
}

func normalizeAuthReason(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "", "none":
		return "none"
	case "unsupported_credentials", "missing_credentials", "invalid_credentials",
		"client_id_not_allowed", "not_authorized", "invalid_will_topic", "will_unauthorized":
		return value
	default:
		return "other"
	}
}

// PacketReceived increments packet received total counter.
func (r *Registry) PacketReceived(packetType string) {
	packetType = normalizePacketType(packetType)
	key := fmt.Sprintf(`packet_type="%s"`, packetType)
	r.getCounter(r.packetsReceivedTotal, key).Add(1)
}

// PacketSent increments packet sent total counter.
func (r *Registry) PacketSent(packetType string) {
	packetType = normalizePacketType(packetType)
	key := fmt.Sprintf(`packet_type="%s"`, packetType)
	r.getCounter(r.packetsSentTotal, key).Add(1)
}

// AuthAttempt increments auth attempts total counter.
func (r *Registry) AuthAttempt(method string, success bool, reason string) {
	method = normalizeAuthMethod(method)
	result := ports.AuthResultFailure
	if success {
		result = ports.AuthResultSuccess
	}
	reason = normalizeAuthReason(reason)
	key := fmt.Sprintf(`method="%s",result="%s",reason="%s"`, method, result, reason)
	r.getCounter(r.authAttemptsTotal, key).Add(1)
}

// ConnectRejected increments the fixed-cardinality MQTT 3.1.0 rejection counter.
func (r *Registry) ConnectRejected(protocol string, reason string) {
	if protocol != ports.ProtocolMQTT310 || reason != ports.ConnectRejectReasonUnsupportedProtocolVersion {
		return
	}
	r.connectRejectedTotal.Add(1)
}

// NodeDrainTransition increments the committed local-node drain phase counter.
func (r *Registry) NodeDrainTransition(phase string) {
	switch phase {
	case "EXCLUDING_TRAFFIC", "WAITING_FOR_LB", "VERIFYING_SESSION_STATE", "TRANSFERRING_SESSIONS",
		"VERIFYING_BARRIER", "COMPLETED", "FAILED", "CANCELLED":
	default:
		return
	}
	r.getCounter(r.nodeDrainTransitions, fmt.Sprintf(`phase="%s"`, phase)).Add(1)
}

// RecordTenantQuotaRejection increments a bounded tenant-and-dimension counter and returns its rendered tenant label.
func (r *Registry) RecordTenantQuotaRejection(tenantID, dimension string) string {
	switch dimension {
	case "retained_bytes", "offline_queue_messages", "offline_queue_bytes", "storage_bytes":
	default:
		return ""
	}
	if _, err := tenancy.NewTenantID(tenantID); err != nil || strings.IndexFunc(tenantID, unicode.IsControl) >= 0 {
		tenantID = tenantQuotaOverflowLabel
	}

	r.mu.Lock()
	if _, exists := r.tenantQuotaTenants[tenantID]; !exists {
		if len(r.tenantQuotaTenants) >= maxTenantQuotaMetricTenants {
			tenantID = tenantQuotaOverflowLabel
		} else {
			r.tenantQuotaTenants[tenantID] = struct{}{}
		}
	}
	key := tenantQuotaMetricKey{tenantID: tenantID, dimension: dimension}
	counter := r.tenantQuotaRejections[key]
	if counter == nil {
		counter = &atomic.Int64{}
		r.tenantQuotaRejections[key] = counter
	}
	counter.Add(1)
	r.mu.Unlock()
	return tenantID
}

// SetTenantQuotaProfile updates gauges describing the locally committed quota profile.
func (r *Registry) SetTenantQuotaProfile(committed bool, version uint64, configMismatch bool) {
	var committedValue, mismatchValue int64
	if committed {
		committedValue = 1
	} else {
		version = 0
	}
	if committed && configMismatch {
		mismatchValue = 1
	}
	r.tenantQuotaProfileCommitted.Store(committedValue)
	r.tenantQuotaProfileVersion.Store(version)
	r.tenantQuotaConfigMismatch.Store(mismatchValue)
}

// SetSessionsActive updates session gauges.
func (r *Registry) SetSessionsActive(persistent bool, count int64) {
	if count < 0 {
		count = 0
	}
	if persistent {
		r.sessionsActivePersistent.Store(count)
	} else {
		r.sessionsActiveNonPersistent.Store(count)
	}
}

// SetSubscriptionsActive updates subscription gauge.
func (r *Registry) SetSubscriptionsActive(count int64) {
	if count < 0 {
		count = 0
	}
	r.subscriptionsActive.Store(count)
}

// SetRetainedStats updates retained stats gauges.
func (r *Registry) SetRetainedStats(messageCount int64, totalBytes int64) {
	if messageCount < 0 {
		messageCount = 0
	}
	if totalBytes < 0 {
		totalBytes = 0
	}
	r.retainedMessages.Store(messageCount)
	r.retainedBytes.Store(totalBytes)
}

// ObservePublishDuration records publish duration observation.
func (r *Registry) ObservePublishDuration(duration time.Duration) {
	r.publishLatency.Observe(duration.Seconds())
}

// ObserveWALSync records one successful dynamic Raft WAL group fsync.
func (r *Registry) ObserveWALSync(duration time.Duration, entries int, frameBytes int) {
	r.walSyncDuration.Observe(duration.Seconds())
	r.walSyncEntries.Observe(float64(entries))
	if frameBytes < 0 {
		frameBytes = 0
	}
	r.walWrittenBytes.Add(uint64(frameBytes))
}

// ActiveConnections returns the current number of active connections.
func (r *Registry) ActiveConnections() int64 {
	v := r.connectionsActive.Load()
	if v < 0 {
		return 0
	}
	return v
}

// Render renders the metrics either in OpenMetrics format (isOpenMetrics=true) or Prometheus v0.0.4.
func (r *Registry) Render(isOpenMetrics bool) []byte {
	var buf bytes.Buffer

	// 1. narumq_connections_active
	fmt.Fprintf(&buf, "# HELP narumq_connections_active Current number of active client connections.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_connections_active gauge\n")
	fmt.Fprintf(&buf, "narumq_connections_active %d\n", r.ActiveConnections())

	// 2. narumq_connections_total
	fmt.Fprintf(&buf, "# HELP narumq_connections_total Total client connection attempts.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_connections_total counter\n")
	r.renderCounterMap(&buf, "narumq_connections_total", r.connectionsTotal)

	// 3. narumq_sessions_active
	fmt.Fprintf(&buf, "# HELP narumq_sessions_active Current number of active client sessions.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_sessions_active gauge\n")
	fmt.Fprintf(&buf, "narumq_sessions_active{persistent=\"true\"} %d\n", r.sessionsActivePersistent.Load())
	fmt.Fprintf(&buf, "narumq_sessions_active{persistent=\"false\"} %d\n", r.sessionsActiveNonPersistent.Load())

	// 4. narumq_subscriptions_active
	fmt.Fprintf(&buf, "# HELP narumq_subscriptions_active Current number of active subscriptions.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_subscriptions_active gauge\n")
	fmt.Fprintf(&buf, "narumq_subscriptions_active %d\n", r.subscriptionsActive.Load())

	// 5. narumq_retained_messages
	fmt.Fprintf(&buf, "# HELP narumq_retained_messages Current number of retained messages.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_retained_messages gauge\n")
	fmt.Fprintf(&buf, "narumq_retained_messages %d\n", r.retainedMessages.Load())

	// 6. narumq_retained_bytes
	fmt.Fprintf(&buf, "# HELP narumq_retained_bytes Total bytes stored by retained messages.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_retained_bytes gauge\n")
	fmt.Fprintf(&buf, "narumq_retained_bytes %d\n", r.retainedBytes.Load())

	// 7. narumq_packets_received_total
	fmt.Fprintf(&buf, "# HELP narumq_packets_received_total Total MQTT packets received by packet type.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_packets_received_total counter\n")
	r.renderCounterMap(&buf, "narumq_packets_received_total", r.packetsReceivedTotal)

	// 8. narumq_packets_sent_total
	fmt.Fprintf(&buf, "# HELP narumq_packets_sent_total Total MQTT packets sent by packet type.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_packets_sent_total counter\n")
	r.renderCounterMap(&buf, "narumq_packets_sent_total", r.packetsSentTotal)

	// 9. narumq_auth_attempts_total
	fmt.Fprintf(&buf, "# HELP narumq_auth_attempts_total Total authentication attempts.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_auth_attempts_total counter\n")
	r.renderCounterMap(&buf, "narumq_auth_attempts_total", r.authAttemptsTotal)

	// 10. narumq_mqtt_connect_rejected_total
	fmt.Fprintf(&buf, "# HELP narumq_mqtt_connect_rejected_total MQTT CONNECT attempts rejected by compatibility reason.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_mqtt_connect_rejected_total counter\n")
	fmt.Fprintf(&buf, "narumq_mqtt_connect_rejected_total{protocol=\"3.1.0\",reason=\"unsupported_protocol_version\"} %d\n", r.connectRejectedTotal.Load())
	// 11. narumq_node_drain_transitions_total
	fmt.Fprintf(&buf, "# HELP narumq_node_drain_transitions_total Committed node drain phase transitions observed for the local node.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_node_drain_transitions_total counter\n")
	r.renderCounterMap(&buf, "narumq_node_drain_transitions_total", r.nodeDrainTransitions)
	fmt.Fprintf(&buf, "# HELP narumq_tenant_quota_profile_committed Whether a tenant quota profile is committed locally.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_tenant_quota_profile_committed gauge\n")
	fmt.Fprintf(&buf, "narumq_tenant_quota_profile_committed %d\n", r.tenantQuotaProfileCommitted.Load())
	fmt.Fprintf(&buf, "# HELP narumq_tenant_quota_profile_version Version of the locally committed tenant quota profile.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_tenant_quota_profile_version gauge\n")
	fmt.Fprintf(&buf, "narumq_tenant_quota_profile_version %d\n", r.tenantQuotaProfileVersion.Load())
	fmt.Fprintf(&buf, "# HELP narumq_tenant_quota_config_mismatch Whether the local quota configuration conflicts with the committed profile.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_tenant_quota_config_mismatch gauge\n")
	fmt.Fprintf(&buf, "narumq_tenant_quota_config_mismatch %d\n", r.tenantQuotaConfigMismatch.Load())
	fmt.Fprintf(&buf, "# HELP narumq_tenant_quota_rejections_total Committed storage mutations rejected by tenant quota dimension.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_tenant_quota_rejections_total counter\n")
	r.renderTenantQuotaRejections(&buf)

	// 11. narumq_publish_latency_seconds
	fmt.Fprintf(&buf, "# HELP narumq_publish_latency_seconds Latency of publish processing in seconds.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_publish_latency_seconds histogram\n")
	renderHistogram(&buf, "narumq_publish_latency_seconds", r.publishLatency)

	fmt.Fprintf(&buf, "# HELP narumq_raft_wal_sync_duration_seconds Duration of one successful Raft WAL group fsync.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_raft_wal_sync_duration_seconds histogram\n")
	renderHistogram(&buf, "narumq_raft_wal_sync_duration_seconds", r.walSyncDuration)
	fmt.Fprintf(&buf, "# HELP narumq_raft_wal_sync_entries Raft log entries persisted by one WAL group fsync.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_raft_wal_sync_entries histogram\n")
	renderHistogram(&buf, "narumq_raft_wal_sync_entries", r.walSyncEntries)
	fmt.Fprintf(&buf, "# HELP narumq_raft_wal_written_bytes_total Bytes appended to the Raft WAL including frame headers.\n")
	fmt.Fprintf(&buf, "# TYPE narumq_raft_wal_written_bytes_total counter\n")
	fmt.Fprintf(&buf, "narumq_raft_wal_written_bytes_total %d\n", r.walWrittenBytes.Load())
	r.renderRaftStages(&buf)

	if isOpenMetrics {
		fmt.Fprintf(&buf, "# EOF\n")
	}

	return buf.Bytes()
}

func renderHistogram(buf *bytes.Buffer, name string, histogram *Histogram) {
	buckets, counts, count, sum := histogram.Snapshot()
	for i, bucket := range buckets {
		fmt.Fprintf(buf, "%s_bucket{le=\"%g\"} %d\n", name, bucket, counts[i])
	}
	fmt.Fprintf(buf, "%s_bucket{le=\"+Inf\"} %d\n", name, count)
	fmt.Fprintf(buf, "%s_sum %.6f\n", name, sum)
	fmt.Fprintf(buf, "%s_count %d\n", name, count)
}

func (r *Registry) renderCounterMap(buf *bytes.Buffer, name string, m map[string]*atomic.Int64) {
	r.mu.RLock()
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]struct {
		labels string
		val    int64
	}, len(keys))
	for i, k := range keys {
		entries[i].labels = k
		entries[i].val = m[k].Load()
	}
	r.mu.RUnlock()

	for _, e := range entries {
		fmt.Fprintf(buf, "%s{%s} %d\n", name, e.labels, e.val)
	}
}
func (r *Registry) renderTenantQuotaRejections(buf *bytes.Buffer) {
	r.mu.RLock()
	entries := make([]tenantQuotaMetricEntry, 0, len(r.tenantQuotaRejections))
	for key, counter := range r.tenantQuotaRejections {
		entries = append(entries, tenantQuotaMetricEntry{
			tenantID: key.tenantID, dimension: key.dimension, count: counter.Load(),
		})
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].tenantID != entries[right].tenantID {
			return entries[left].tenantID < entries[right].tenantID
		}
		return entries[left].dimension < entries[right].dimension
	})
	for _, entry := range entries {
		fmt.Fprintf(buf, "narumq_tenant_quota_rejections_total{tenant=\"%s\",dimension=\"%s\"} %d\n",
			prometheusLabelEscaper.Replace(entry.tenantID), prometheusLabelEscaper.Replace(entry.dimension), entry.count)
	}
}
