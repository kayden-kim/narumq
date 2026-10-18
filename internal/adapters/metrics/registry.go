package metrics

import (
	"bytes"
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
type connectionMetricKey struct {
	protocol  string
	transport string
	result    string
}

type packetMetricKey struct {
	packetType string
}

type authAttemptMetricKey struct {
	method string
	result string
	reason string
}

type nodeDrainMetricKey struct {
	phase string
}

type tenantQuotaMetricEntry struct {
	tenantID  string
	dimension string
	count     int64
}

var prometheusLabelEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")

var willOutcomeReasons = [...]string{
	ports.WillOutcomeSuppressedNormalDisconnect,
	ports.WillOutcomeSuppressedReconnectedWithinDelay,
	ports.WillOutcomeExpectedMQTT311ServerDisconnect,
	ports.WillOutcomeExpectedZeroOrExpiredWillDelay,
	ports.WillOutcomeUnexpectedSupportedClient,
	ports.WillOutcomeUnplannedFailure,
}
var auditOutcomes = [...]string{
	ports.AuditOutcomePublished,
	ports.AuditOutcomePublishFailed,
	ports.AuditOutcomeInvalid,
	ports.AuditOutcomeDenialSuppressed,
	ports.AuditOutcomeMutationRejected,
	ports.AuditOutcomeDataPlaneSuppressed,
	ports.AuditOutcomeDataPlaneDropped,
}

var listenerAdmissionTransports = [...]string{ports.TransportTCP, ports.TransportTLS, ports.TransportWS, ports.TransportWSS}
var listenerAdmissionReasons = [...]string{ports.ListenerAdmissionReasonMaxConnections, ports.ListenerAdmissionReasonConnectionRate}
var connectRejectedProtocols = [...]string{ports.ProtocolMQTT311, ports.ProtocolMQTT50}
var connectRejectReasons = [...]string{
	ports.ConnectRejectReasonUnsupportedProtocolVersion,
	ports.ConnectRejectReasonIdentifierRejected,
	ports.ConnectRejectReasonBadCredentials,
	ports.ConnectRejectReasonNotAuthorized,
	ports.ConnectRejectReasonServerUnavailable,
	ports.ConnectRejectReasonQuotaExceeded,
	ports.ConnectRejectReasonBanned,
	ports.ConnectRejectReasonOther,
}
var messageDropReasons = [...]string{
	ports.MessageDropReasonQueueFull,
	ports.MessageDropReasonPacketTooLarge,
}
var nodeDrainProgressQuantities = [...]string{
	"connections_initial",
	"connections_remaining",
	"sessions_ready_for_takeover",
	"sessions_taken_over",
	"sessions_failed",
	"pending_wal",
	"under_replicated",
}

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

// SubscriptionCounter reports the current number of routed subscriptions.
type SubscriptionCounter interface {
	SubscriptionCount() int
}

// NodeDrainProgress contains committed progress values for the local node's drain.
type NodeDrainProgress struct {
	// ConnectionsInitial is the local connection count when drain began.
	ConnectionsInitial uint64
	// ConnectionsRemaining is the current local connection count.
	ConnectionsRemaining uint64
	// SessionsReadyForTakeover is the number of sessions ready for transfer.
	SessionsReadyForTakeover uint64
	// SessionsTakenOver is the number of sessions transferred.
	SessionsTakenOver uint64
	// SessionsFailed is the number of session transfers that failed.
	SessionsFailed uint64
	// PendingWAL is the number of pending local WAL operations.
	PendingWAL uint64
	// UnderReplicated is the number of under-replicated items.
	UnderReplicated uint64
}

// Registry stores operational metrics and renders them in Prometheus / OpenMetrics text formats.
type Registry struct {
	mu sync.RWMutex

	startedAt           time.Time
	metricCatalog       []ports.MetricDescriptorDTO
	subscriptionCounter SubscriptionCounter

	connectionsActive atomic.Int64
	connectionsTotal  map[connectionMetricKey]*atomic.Int64

	sessionsActivePersistent    atomic.Int64
	sessionsActiveNonPersistent atomic.Int64

	retainedMessages atomic.Int64
	retainedBytes    atomic.Int64

	packetsReceivedTotal          map[packetMetricKey]*atomic.Int64
	packetsSentTotal              map[packetMetricKey]*atomic.Int64
	authAttemptsTotal             map[authAttemptMetricKey]*atomic.Int64
	connectRejectedTotal          atomic.Int64
	connectRejectedByProtocol     [len(connectRejectedProtocols)][len(connectRejectReasons)]atomic.Uint64
	nodeDrainTransitions          map[nodeDrainMetricKey]*atomic.Int64
	messageDrops                  [len(messageDropReasons)]atomic.Uint64
	nodeDrainActive               atomic.Bool
	nodeDrainProgress             [len(nodeDrainProgressQuantities)]atomic.Uint64
	willOutcomes                  [len(willOutcomeReasons)]atomic.Int64
	auditOutcomeCounts            [len(auditOutcomes)]atomic.Int64
	auditPublisherReady           atomic.Bool
	listenerAdmissionRejections   [len(listenerAdmissionTransports)][len(listenerAdmissionReasons)]atomic.Int64
	tenantQuotaRejections         map[tenantQuotaMetricKey]*atomic.Int64
	tenantLimitRejections         map[tenantQuotaMetricKey]*atomic.Int64
	tenantQuotaTenants            map[string]struct{}
	tenantQuotaProfileCommitted   atomic.Int64
	tenantQuotaProfileVersion     atomic.Uint64
	tenantQuotaConfigMismatch     atomic.Int64
	clusterPolicyCommitted        atomic.Int64
	clusterPolicyVersion          atomic.Uint64
	clusterPolicyConfigMismatch   atomic.Int64
	publishBackpressureRejections [len(publishBackpressureReasons)]atomic.Uint64
	publishBackpressureActive     [2]atomic.Bool
	storageFreeBytes              atomic.Uint64
	storageFreeBytesSet           atomic.Bool
	commitBacklogOldest           atomic.Int64
	durableDeliverySaturated      atomic.Uint64
	peerReplicationLag            []peerReplicationLagEntry

	publishLatency  *Histogram
	walSyncDuration *Histogram
	walSyncEntries  *Histogram
	walWrittenBytes atomic.Uint64
	raftStages      [raftStageCount]*Histogram
}

// NewRegistry initializes an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		startedAt:             time.Now().UTC(),
		metricCatalog:         ports.MetricCatalog(),
		connectionsTotal:      make(map[connectionMetricKey]*atomic.Int64),
		packetsReceivedTotal:  make(map[packetMetricKey]*atomic.Int64),
		packetsSentTotal:      make(map[packetMetricKey]*atomic.Int64),
		authAttemptsTotal:     make(map[authAttemptMetricKey]*atomic.Int64),
		nodeDrainTransitions:  make(map[nodeDrainMetricKey]*atomic.Int64),
		tenantQuotaRejections: make(map[tenantQuotaMetricKey]*atomic.Int64),
		tenantLimitRejections: make(map[tenantQuotaMetricKey]*atomic.Int64),
		tenantQuotaTenants:    make(map[string]struct{}),
		publishLatency:        NewHistogram(defaultPublishLatencyBuckets),
		walSyncDuration:       NewHistogram(defaultWALSyncDurationBuckets),
		walSyncEntries:        NewHistogram(defaultWALSyncEntriesBuckets),
		raftStages:            newRaftStageHistograms(),
	}
}

var _ ports.MetricsRecorder = (*Registry)(nil)
var _ ports.WillOutcomeRecorder = (*Registry)(nil)
var _ ports.AuditOutcomeRecorder = (*Registry)(nil)
var _ ports.ListenerAdmissionRecorder = (*Registry)(nil)

func getCounter[K comparable](r *Registry, counters map[K]*atomic.Int64, key K) *atomic.Int64 {
	r.mu.RLock()
	counter, exists := counters[key]
	r.mu.RUnlock()
	if exists {
		return counter
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if counter, exists = counters[key]; exists {
		return counter
	}
	counter = &atomic.Int64{}
	counters[key] = counter
	return counter
}

// ConnectionOpened increments active connections and total counter.
func (r *Registry) ConnectionOpened(protocol string, transport string) {
	protocol = normalizeProtocol(protocol)
	transport = normalizeTransport(transport)
	r.connectionsActive.Add(1)
	key := connectionMetricKey{protocol: protocol, transport: transport, result: "success"}
	getCounter(r, r.connectionsTotal, key).Add(1)
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
	key := connectionMetricKey{protocol: protocol, transport: transport, result: "closed"}
	getCounter(r, r.connectionsTotal, key).Add(1)
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
		"client_id_not_allowed", "not_authorized", "invalid_will_topic", "will_unauthorized", "blocked":
		return value
	default:
		return "other"
	}
}

// PacketReceived increments packet received total counter.
func (r *Registry) PacketReceived(packetType string) {
	packetType = normalizePacketType(packetType)
	key := packetMetricKey{packetType: packetType}
	getCounter(r, r.packetsReceivedTotal, key).Add(1)
}

// PacketSent increments packet sent total counter.
func (r *Registry) PacketSent(packetType string) {
	packetType = normalizePacketType(packetType)
	key := packetMetricKey{packetType: packetType}
	getCounter(r, r.packetsSentTotal, key).Add(1)
}

// AuthAttempt increments auth attempts total counter.
func (r *Registry) AuthAttempt(method string, success bool, reason string) {
	method = normalizeAuthMethod(method)
	result := ports.AuthResultFailure
	if success {
		result = ports.AuthResultSuccess
	}
	reason = normalizeAuthReason(reason)
	key := authAttemptMetricKey{method: method, result: result, reason: reason}
	getCounter(r, r.authAttemptsTotal, key).Add(1)
}

// ConnectRejected increments a fixed-cardinality CONNECT rejection counter.
func (r *Registry) ConnectRejected(protocol string, reason string) {
	if protocol == ports.ProtocolMQTT310 {
		if reason == ports.ConnectRejectReasonUnsupportedProtocolVersion {
			r.connectRejectedTotal.Add(1)
		}
		return
	}

	protocolIndex := -1
	for index, knownProtocol := range connectRejectedProtocols {
		if protocol == knownProtocol {
			protocolIndex = index
			break
		}
	}
	if protocolIndex < 0 {
		return
	}
	for reasonIndex, knownReason := range connectRejectReasons {
		if reason == knownReason {
			r.connectRejectedByProtocol[protocolIndex][reasonIndex].Add(1)
			return
		}
	}
}

// MessageDropped increments only a registered post-routing delivery drop.
func (r *Registry) MessageDropped(reason string) {
	for index, knownReason := range messageDropReasons {
		if reason == knownReason {
			r.messageDrops[index].Add(1)
			return
		}
	}
}

// SetNodeDrainProgress updates the committed drain gauges for this node.
func (r *Registry) SetNodeDrainProgress(active bool, progress NodeDrainProgress) {
	r.nodeDrainProgress[0].Store(progress.ConnectionsInitial)
	r.nodeDrainProgress[1].Store(progress.ConnectionsRemaining)
	r.nodeDrainProgress[2].Store(progress.SessionsReadyForTakeover)
	r.nodeDrainProgress[3].Store(progress.SessionsTakenOver)
	r.nodeDrainProgress[4].Store(progress.SessionsFailed)
	r.nodeDrainProgress[5].Store(progress.PendingWAL)
	r.nodeDrainProgress[6].Store(progress.UnderReplicated)
	r.nodeDrainActive.Store(active)
}

// NodeDrainTransition increments the committed local-node drain phase counter.
func (r *Registry) NodeDrainTransition(phase string) {
	switch phase {
	case "EXCLUDING_TRAFFIC", "WAITING_FOR_LB", "VERIFYING_SESSION_STATE", "TRANSFERRING_SESSIONS",
		"VERIFYING_BARRIER", "COMPLETED", "FAILED", "CANCELLED":
	default:
		return
	}
	getCounter(r, r.nodeDrainTransitions, nodeDrainMetricKey{phase: phase}).Add(1)
}

// WillOutcome increments only a known final Will outcome.
func (r *Registry) WillOutcome(reason string) {
	for index, known := range willOutcomeReasons {
		if reason == known {
			r.willOutcomes[index].Add(1)
			return
		}
	}
}

// AuditOutcome increments only a registered publication outcome.
func (r *Registry) AuditOutcome(outcome string) {
	for index, known := range auditOutcomes {
		if outcome == known {
			r.auditOutcomeCounts[index].Add(1)
			return
		}
	}
}

// SetAuditPublisherReady reports whether the node-local publisher can append events.
func (r *Registry) SetAuditPublisherReady(ready bool) {
	r.auditPublisherReady.Store(ready)
}

// ListenerAdmissionRejected increments only a known transport and admission reason.
func (r *Registry) ListenerAdmissionRejected(transport string, reason string) {
	for transportIndex, knownTransport := range listenerAdmissionTransports {
		if transport != knownTransport {
			continue
		}
		for reasonIndex, knownReason := range listenerAdmissionReasons {
			if reason == knownReason {
				r.listenerAdmissionRejections[transportIndex][reasonIndex].Add(1)
				return
			}
		}
		return
	}
}

// RecordTenantQuotaRejection increments a bounded tenant-and-dimension counter and returns its rendered tenant label.
func (r *Registry) RecordTenantQuotaRejection(tenantID, dimension string) string {
	switch dimension {
	case "retained_bytes", "offline_queue_messages", "offline_queue_bytes", "storage_bytes":
	default:
		return ""
	}
	return r.recordTenantRejection(r.tenantQuotaRejections, tenantID, dimension)
}

// RecordTenantLimitRejection increments a bounded tenant limit rejection counter and returns its rendered tenant label.
func (r *Registry) RecordTenantLimitRejection(tenantID, dimension string) string {
	switch dimension {
	case "connections", "sessions", "subscriptions", "message_rate", "byte_rate", "payload_size":
	default:
		return ""
	}
	return r.recordTenantRejection(r.tenantLimitRejections, tenantID, dimension)
}

func (r *Registry) recordTenantRejection(counters map[tenantQuotaMetricKey]*atomic.Int64, tenantID, dimension string) string {
	r.mu.Lock()
	tenantID = r.boundedTenantLabelLocked(tenantID)
	key := tenantQuotaMetricKey{tenantID: tenantID, dimension: dimension}
	counter := counters[key]
	if counter == nil {
		counter = &atomic.Int64{}
		counters[key] = counter
	}
	counter.Add(1)
	r.mu.Unlock()
	return tenantID
}

func (r *Registry) boundedTenantLabelLocked(tenantID string) string {
	if _, err := tenancy.NewTenantID(tenantID); err != nil || strings.IndexFunc(tenantID, unicode.IsControl) >= 0 {
		tenantID = tenantQuotaOverflowLabel
	}
	if _, exists := r.tenantQuotaTenants[tenantID]; !exists {
		if len(r.tenantQuotaTenants) >= maxTenantQuotaMetricTenants {
			tenantID = tenantQuotaOverflowLabel
		} else {
			r.tenantQuotaTenants[tenantID] = struct{}{}
		}
	}
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

// SetClusterPolicy updates gauges for the locally applied cluster policy.
func (r *Registry) SetClusterPolicy(committed bool, version uint64, configMismatch bool) {
	var committedValue, mismatchValue int64
	if committed {
		committedValue = 1
	} else {
		version = 0
	}
	if committed && configMismatch {
		mismatchValue = 1
	}
	r.clusterPolicyCommitted.Store(committedValue)
	r.clusterPolicyVersion.Store(version)
	r.clusterPolicyConfigMismatch.Store(mismatchValue)
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

// ObserveSubscriptions makes Render read the active subscription count at scrape time.
func (r *Registry) ObserveSubscriptions(counter SubscriptionCounter) {
	r.mu.Lock()
	r.subscriptionCounter = counter
	r.mu.Unlock()
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

// Render renders the metrics either in OpenMetrics format or Prometheus v0.0.4.
func (r *Registry) Render(isOpenMetrics bool) []byte {
	var buffer bytes.Buffer
	visitor := textMetricVisitor{buffer: &buffer}
	if err := r.VisitMetrics(&visitor); err != nil {
		panic(err)
	}
	if isOpenMetrics {
		buffer.WriteString("# EOF\n")
	}
	return buffer.Bytes()
}
