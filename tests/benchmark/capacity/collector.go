package main

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

// windowID labels step time. Counters and latency samples are attributed to the window in
// which the event (receipt, timeout, disconnect) was observed.
type windowID int32

const (
	winSetup windowID = iota
	winWarmup
	winMeasureN
	winFailure
	winMeasureN1
	winRestore
	winCleanup
	numWindows
)

var windowNames = [numWindows]string{"setup", "warmup", "measureN", "failure", "measureN1", "restore", "cleanup"}

func (w windowID) String() string { return windowNames[w] }

const (
	mfSent byte = 1 << iota
	mfAcked
	mfInterrupted
	mfFailed
)

// msgState is the bounded per-message oracle record (24 bytes).
type msgState struct {
	sent       int64  // nanoseconds since the step base, taken just before the write
	ackMicros  uint32 // PUBACK/PUBREC latency
	firstMicro uint32 // first-copy delivery latency
	rcv        uint8  // recipients that received a first copy
	flags      byte
}

const pendingSuccessCap = 32

type streamState struct {
	mu             sync.Mutex
	plan           *streamPlan
	cell, stream   int
	msgs           []msgState
	firstSuccess   int64 // first success at or after the injection, 0 if none
	pendingSuccess []int64
}

type windowCounters struct {
	ackTimeouts, protocolErrors, unplannedDisconnects atomic.Uint64
	scheduleOverloads, creditStarvations              atomic.Uint64
	skippedDisconnected, reconnects                   atomic.Uint64
	published, delivered                              [3]atomic.Uint64
}

type outageKey struct{ cell, stream, index int }

type outage struct{ start, end int64 } // end < 0 while open

type collector struct {
	profile Profile
	token   uint32
	base    time.Time
	cells   int
	plans   []*cellPlan
	streams []*streamState

	cur  atomic.Int32
	win  [numWindows]windowCounters
	hist [2][3]*histogram

	duplicates      [3]atomic.Uint64
	unexpected      atomic.Uint64
	stale           atomic.Uint64
	retainedReplays atomic.Uint64
	outstanding     atomic.Int64

	sessionPresentFailures atomic.Uint64
	tenantRejections       atomic.Uint64
	setupFailures          atomic.Uint64

	t0            atomic.Int64 // killing marker observation, -1 until observed
	injection     atomic.Int64 // earliest attributable disconnect or down marker, -1 until known
	clientsDown   atomic.Int64
	affected      atomic.Uint64
	lastReconnect atomic.Int64

	outageMu sync.Mutex
	outages  map[outageKey][]outage
}

func newCollector(profile Profile, token uint32, plans []*cellPlan) *collector {
	c := &collector{
		profile: profile, token: token, base: time.Now(), cells: len(plans), plans: plans,
		streams: make([]*streamState, len(plans)*cellStreams), outages: make(map[outageKey][]outage),
	}
	for cell, plan := range plans {
		for stream := range cellStreams {
			c.streams[cell*cellStreams+stream] = &streamState{
				plan: &plan.streams[stream], cell: cell, stream: stream, msgs: make([]msgState, 0, 64),
			}
		}
	}
	for w := range c.hist {
		for q := range c.hist[w] {
			c.hist[w][q] = newHistogram()
		}
	}
	c.t0.Store(-1)
	c.injection.Store(-1)
	c.cur.Store(int32(winSetup))
	return c
}

func (c *collector) rel(t time.Time) int64 { return int64(t.Sub(c.base)) }

func (c *collector) now() int64 { return int64(time.Since(c.base)) }

func (c *collector) setWindow(w windowID) { c.cur.Store(int32(w)) }

func (c *collector) window() windowID { return windowID(c.cur.Load()) }

func (c *collector) counters() *windowCounters { return &c.win[c.window()] }

func (c *collector) stream(cell, stream int) *streamState { return c.streams[cell*cellStreams+stream] }

// begin reserves the next sequence of a stream and stamps the publish-before time.
func (c *collector) begin(cell, stream int) (uint64, int64) {
	st := c.stream(cell, stream)
	now := c.now()
	st.mu.Lock()
	st.msgs = append(st.msgs, msgState{sent: now, flags: mfSent})
	seq := uint64(len(st.msgs))
	st.mu.Unlock()
	c.counters().published[st.plan.def.qos].Add(1)
	return seq, now
}

func (c *collector) publishFailed(cell, stream int, seq uint64) {
	st := c.stream(cell, stream)
	st.mu.Lock()
	st.msgs[seq-1].flags |= mfFailed
	st.mu.Unlock()
}

func (c *collector) publishInterrupted(cell, stream int, seq uint64) {
	st := c.stream(cell, stream)
	st.mu.Lock()
	st.msgs[seq-1].flags |= mfInterrupted
	st.mu.Unlock()
}

// ack records the broker's acknowledgement: PUBACK for QoS 1, PUBREC for QoS 2.
func (c *collector) ack(cell, stream int, seq uint64) {
	st := c.stream(cell, stream)
	now := c.now()
	st.mu.Lock()
	m := &st.msgs[seq-1]
	if m.flags&mfAcked == 0 {
		m.flags |= mfAcked
		m.ackMicros = uint32(max(now-m.sent, 0) / int64(time.Microsecond))
		rcp := st.plan.recipients[(seq-1)%uint64(len(st.plan.recipients))]
		c.outstanding.Add(int64(rcp.expected() - receivedCount(rcp, m.rcv)))
	}
	if st.plan.def.qos == 1 {
		st.noteSuccess(now, c)
	}
	st.mu.Unlock()
}

// complete records QoS 2 PUBCOMP as a stream success.
func (c *collector) complete(cell, stream int) {
	st := c.stream(cell, stream)
	now := c.now()
	st.mu.Lock()
	st.noteSuccess(now, c)
	st.mu.Unlock()
}

func receivedCount(r recipient, received uint8) int {
	if r.shared {
		return min(1, bits.OnesCount8(received&r.mask))
	}
	return bits.OnesCount8(received & r.mask)
}

type deliverKind int

const (
	deliverFirst deliverKind = iota
	deliverDuplicate
	deliverUnexpected
)

// deliver records one received copy for subscriber sub of a stream.
func (c *collector) deliver(cell, stream, sub int, seq uint64) deliverKind {
	st := c.stream(cell, stream)
	now := c.now()
	qos := st.plan.def.qos
	st.mu.Lock()
	if seq == 0 || seq > uint64(len(st.msgs)) {
		st.mu.Unlock()
		c.unexpected.Add(1)
		return deliverUnexpected
	}
	m := &st.msgs[seq-1]
	rcp := st.plan.recipients[(seq-1)%uint64(len(st.plan.recipients))]
	bit := uint8(1) << sub
	if rcp.mask&bit == 0 {
		st.mu.Unlock()
		c.unexpected.Add(1)
		return deliverUnexpected
	}
	first := m.rcv&bit == 0
	if rcp.shared {
		first = m.rcv&rcp.mask == 0
	}
	m.rcv |= bit
	if !first {
		st.mu.Unlock()
		c.duplicates[qos].Add(1)
		return deliverDuplicate
	}
	latency := now - m.sent
	m.firstMicro = uint32(max(latency, 0)/int64(time.Microsecond)) + 1
	if m.flags&mfAcked != 0 && qos > 0 {
		c.outstanding.Add(-1)
	}
	if qos == 0 {
		st.noteSuccess(now, c)
	}
	st.mu.Unlock()
	w := c.window()
	c.win[w].delivered[qos].Add(1)
	switch w {
	case winMeasureN:
		c.hist[0][qos].Record(time.Duration(latency))
	case winMeasureN1:
		c.hist[1][qos].Record(time.Duration(latency))
	}
	return deliverFirst
}

// noteSuccess must be called with st.mu held.
func (st *streamState) noteSuccess(now int64, c *collector) {
	if st.firstSuccess != 0 {
		return
	}
	t0 := c.t0.Load()
	if t0 < 0 || now < t0 {
		return
	}
	if injection := c.injection.Load(); injection >= 0 {
		if now >= injection {
			st.firstSuccess = max(now, 1)
		}
		return
	}
	if len(st.pendingSuccess) == pendingSuccessCap {
		copy(st.pendingSuccess, st.pendingSuccess[1:])
		st.pendingSuccess = st.pendingSuccess[:pendingSuccessCap-1]
	}
	st.pendingSuccess = append(st.pendingSuccess, now)
}

// observeKilling records the killing-marker observation that opens the failure attribution.
func (c *collector) observeKilling() { c.t0.CompareAndSwap(-1, c.now()) }

// setInjection fixes the failure injection time once and settles per-stream pending successes.
func (c *collector) setInjection(at int64) bool {
	if !c.injection.CompareAndSwap(-1, at) {
		return false
	}
	for _, st := range c.streams {
		st.mu.Lock()
		if st.firstSuccess == 0 {
			for _, t := range st.pendingSuccess {
				if t >= at {
					st.firstSuccess = max(t, 1)
					break
				}
			}
		}
		st.pendingSuccess = nil
		st.mu.Unlock()
	}
	return true
}

func (c *collector) unplannedDisconnect() {
	now := c.now()
	c.counters().unplannedDisconnects.Add(1)
	c.clientsDown.Add(1)
	if t0 := c.t0.Load(); t0 >= 0 && now >= t0 {
		c.affected.Add(1)
		c.setInjection(now)
	}
}

func (c *collector) reconnected() {
	c.clientsDown.Add(-1)
	c.counters().reconnects.Add(1)
	now := c.now()
	for {
		current := c.lastReconnect.Load()
		if now <= current || c.lastReconnect.CompareAndSwap(current, now) {
			return
		}
	}
}

// recovery reports whether every affected client is connected and every stream completed a
// success at or after the injection, with the resulting RTO measured from the injection.
func (c *collector) recovery() (rto time.Duration, recovered bool, injected bool) {
	injection := c.injection.Load()
	if injection < 0 {
		return 0, false, false
	}
	last := max(c.lastReconnect.Load(), injection)
	for _, st := range c.streams {
		st.mu.Lock()
		first := st.firstSuccess
		st.mu.Unlock()
		if first == 0 {
			return time.Duration(c.now() - injection), false, true
		}
		last = max(last, first)
	}
	if c.clientsDown.Load() != 0 {
		return time.Duration(c.now() - injection), false, true
	}
	return time.Duration(last - injection), true, true
}

// subscriberDown and subscriberUp bracket the outage of a clean-session QoS 1 subscriber:
// from the observed connection loss until the SUBACK of its new connection.
func (c *collector) subscriberDown(cell, stream, index int) {
	c.subscriberDownAt(cell, stream, index, c.now())
}

func (c *collector) subscriberDownAt(cell, stream, index int, at int64) {
	c.outageMu.Lock()
	key := outageKey{cell, stream, index}
	c.outages[key] = append(c.outages[key], outage{start: at, end: -1})
	c.outageMu.Unlock()
}

func (c *collector) subscriberUp(cell, stream, index int) {
	c.subscriberUpAt(cell, stream, index, c.now())
}

func (c *collector) subscriberUpAt(cell, stream, index int, at int64) {
	c.outageMu.Lock()
	key := outageKey{cell, stream, index}
	if list := c.outages[key]; len(list) > 0 && list[len(list)-1].end < 0 {
		list[len(list)-1].end = at
	}
	c.outageMu.Unlock()
}

type missingClass int

const (
	missingGap missingClass = iota
	missingNotExpected
	missingCleanSessionLoss
)

// classifyMissing decides what a missing delivery of a message sent at sentNs means for one
// subscriber. A persistent-session subscriber is guaranteed the delivery: always a gap. A
// clean-session subscriber is guaranteed it only while its connection stays up: it is not an
// expected recipient if the publish was sent while it was disconnected (after the disconnect
// and not later than its new SUBACK), and a missing delivery after a connection loss that
// followed the publish is a cleanSessionLoss, whatever its age. delta is the first such
// disconnect minus the publish time.
func (c *collector) classifyMissing(cell, stream, index int, sentNs int64) (class missingClass, delta int64) {
	def := streamDefs[stream]
	if def.persistent || def.qos == 0 {
		return missingGap, 0
	}
	c.outageMu.Lock()
	defer c.outageMu.Unlock()
	best := int64(-1)
	for _, o := range c.outages[outageKey{cell, stream, index}] {
		if sentNs > o.start && (o.end < 0 || sentNs <= o.end) {
			return missingNotExpected, 0
		}
		if d := o.start - sentNs; d >= 0 && (best < 0 || d < best) {
			best = d
		}
	}
	if best >= 0 {
		return missingCleanSessionLoss, best
	}
	return missingGap, 0
}

// classifySharedMissing applies the same rule to a shared-group message that no member
// received: it is not expected when no member was connected, a clean-session loss when an
// active member lost its connection after the publish, and a gap otherwise.
func (c *collector) classifySharedMissing(cell, stream int, rcp recipient, sentNs int64) (class missingClass, delta int64) {
	active, best := 0, int64(-1)
	for sub := range streamDefs[stream].subscribers {
		if rcp.mask&(1<<sub) == 0 {
			continue
		}
		memberClass, d := c.classifyMissing(cell, stream, sub, sentNs)
		switch memberClass {
		case missingNotExpected:
			continue
		case missingCleanSessionLoss:
			if best < 0 || d < best {
				best = d
			}
		}
		active++
	}
	switch {
	case active == 0:
		return missingNotExpected, 0
	case best >= 0:
		return missingCleanSessionLoss, best
	default:
		return missingGap, 0
	}
}

type windowStats struct {
	AckTimeouts          uint64    `json:"ackTimeouts"`
	ProtocolErrors       uint64    `json:"protocolErrors"`
	UnplannedDisconnects uint64    `json:"unplannedDisconnects"`
	ScheduleOverloads    uint64    `json:"generatorScheduleOverloads"`
	CreditStarvations    uint64    `json:"publishCreditStarvations"`
	SkippedDisconnected  uint64    `json:"publishSkippedWhileDisconnected"`
	Reconnects           uint64    `json:"reconnects"`
	Published            [3]uint64 `json:"publishedByQoS"`
	Delivered            [3]uint64 `json:"firstCopyDeliveriesByQoS"`
}

func (c *collector) windowStats(w windowID) windowStats {
	counters := &c.win[w]
	stats := windowStats{
		AckTimeouts: counters.ackTimeouts.Load(), ProtocolErrors: counters.protocolErrors.Load(),
		UnplannedDisconnects: counters.unplannedDisconnects.Load(), ScheduleOverloads: counters.scheduleOverloads.Load(),
		CreditStarvations: counters.creditStarvations.Load(), SkippedDisconnected: counters.skippedDisconnected.Load(),
		Reconnects: counters.reconnects.Load(),
	}
	for qos := range 3 {
		stats.Published[qos] = counters.published[qos].Load()
		stats.Delivered[qos] = counters.delivered[qos].Load()
	}
	return stats
}

// messageRecord is a sampled or problematic message written to messages-sample.jsonl.
type messageRecord struct {
	Cell           int    `json:"cell"`
	Stream         string `json:"stream"`
	Sequence       uint64 `json:"sequence"`
	QoS            byte   `json:"qos"`
	Topic          string `json:"topic"`
	SentNanos      int64  `json:"sentNanos"`
	AckMicros      uint32 `json:"ackMicros,omitempty"`
	FirstCopyMicro uint32 `json:"firstCopyMicros,omitempty"`
	Expected       int    `json:"expectedRecipients"`
	Received       int    `json:"receivedRecipients"`
	Acked          bool   `json:"acked"`
	Interrupted    bool   `json:"interrupted,omitempty"`
	Outcome        string `json:"outcome"`
}

// cleanSessionLoss counts missing deliveries excluded from gaps by the clean-session crash
// window, with the largest disconnect-minus-publish time among them.
type cleanSessionLoss struct {
	Count     uint64  `json:"count"`
	MaxMillis float64 `json:"maxDisconnectAfterPublishMillis"`
	// OverLimit counts losses whose disconnect-minus-publish age exceeds the latency limit;
	// the verdict fails on them.
	OverLimit uint64 `json:"olderThanLatencyLimit"`
}

// gapResult is the whole-step delivery oracle result.
type gapResult struct {
	Acked            [3]uint64           `json:"ackedMessagesByQoS"`
	Gaps             [3]uint64           `json:"ackedGapsByQoS"`
	CleanSessionLoss [3]cleanSessionLoss `json:"cleanSessionLoss"`
	NotExpected      [3]uint64           `json:"notExpectedWhileDisconnectedByQoS"`
	Unacked          [3]uint64           `json:"unackedMessagesByQoS"`
	AckedInterrupted [3]uint64           `json:"ackedButExchangeInterruptedByQoS"`
	// CleanPublisherInterrupted counts missing deliveries of QoS 2 messages whose clean-session
	// publisher saw PUBREC but lost its connection before PUBCOMP: not guaranteed, not a gap.
	CleanPublisherInterrupted uint64    `json:"cleanPublisherInterrupted"`
	QoS0Lost                  uint64    `json:"qos0LostDeliveries"`
	QoS0Expected              uint64    `json:"qos0ExpectedDeliveries"`
	Sent                      [3]uint64 `json:"sentMessagesByQoS"`
	SampleRecords             uint64    `json:"messageRecordsWritten"`
}

// lossOverLimit is the number of clean-session losses older than the latency limit.
func lossOverLimit(r gapResult) (total uint64) {
	for _, loss := range r.CleanSessionLoss {
		total += loss.OverLimit
	}
	return total
}

// lossMaxMillis is the largest clean-session loss age across QoS levels.
func lossMaxMillis(r gapResult) (maxAge float64) {
	for _, loss := range r.CleanSessionLoss {
		maxAge = max(maxAge, loss.MaxMillis)
	}
	return maxAge
}

const (
	sampleModulus     = 2048
	maxProblemRecords = 500
)

// evaluate computes the ACKed-gap oracle over every stream and emits sampled and problematic
// message records. Sampling rule: every message whose (cell,stream,sequence) hash is divisible
// by sampleModulus, plus the first maxProblemRecords gap or unexpected-state messages.
// lossLimit is the age above which a clean-session loss is counted as over the latency limit.
func (c *collector) evaluate(lossLimit time.Duration, emit func(messageRecord)) gapResult {
	var result gapResult
	problems := 0
	for _, st := range c.streams {
		def := st.plan.def
		st.mu.Lock()
		for index := range st.msgs {
			m := st.msgs[index]
			if m.flags&mfFailed != 0 {
				continue
			}
			seq := uint64(index + 1)
			qos := def.qos
			rcp := st.plan.recipients[index%len(st.plan.recipients)]
			result.Sent[qos]++
			acked := m.flags&mfAcked != 0
			if acked {
				result.Acked[qos]++
				if m.flags&mfInterrupted != 0 {
					result.AckedInterrupted[qos]++
				}
			} else if qos > 0 {
				result.Unacked[qos]++
			}
			expected, received := rcp.expected(), receivedCount(rcp, m.rcv)
			outcome := "delivered"
			switch {
			case received >= expected:
			case qos == 0:
				result.QoS0Lost += uint64(expected - received)
				outcome = "qos0-lost"
			case !acked:
				outcome = "unacked-undelivered"
			default:
				var gaps, losses, notExpected uint64
				// All publishers are clean-session: a QoS 2 exchange cut off between PUBREC and
				// PUBCOMP is not guaranteed, so its would-be gaps are not gaps.
				cleanInterrupted := qos == 2 && m.flags&mfInterrupted != 0
				record := func(class missingClass, delta int64, count uint64) {
					switch {
					case class == missingGap && cleanInterrupted:
						result.CleanPublisherInterrupted += count
					case class == missingGap:
						gaps += count
					case class == missingNotExpected:
						notExpected += count
					default:
						losses += count
						loss := &result.CleanSessionLoss[qos]
						loss.Count += count
						loss.MaxMillis = max(loss.MaxMillis, float64(delta)/float64(time.Millisecond))
						if delta > int64(lossLimit) {
							loss.OverLimit += count
						}
					}
				}
				if rcp.shared {
					class, delta := c.classifySharedMissing(st.cell, st.stream, rcp, m.sent)
					record(class, delta, 1)
				} else {
					for sub := range def.subscribers {
						if rcp.mask&(1<<sub) == 0 || m.rcv&(1<<sub) != 0 {
							continue
						}
						class, delta := c.classifyMissing(st.cell, st.stream, sub, m.sent)
						record(class, delta, 1)
					}
				}
				result.Gaps[qos] += gaps
				result.NotExpected[qos] += notExpected
				switch {
				case gaps > 0:
					outcome = "gap"
				case cleanInterrupted && losses == 0 && notExpected == 0:
					outcome = "clean-publisher-interrupted"
				case losses > 0:
					outcome = "clean-session-loss"
				default:
					outcome = "not-expected-while-disconnected"
				}
			}
			if qos == 0 {
				result.QoS0Expected += uint64(expected)
			}
			sampled := hashMessage(st.cell, st.stream, seq)%sampleModulus == 0
			problem := outcome == "gap" && problems < maxProblemRecords
			if problem {
				problems++
			}
			if emit != nil && (sampled || problem) {
				result.SampleRecords++
				emit(messageRecord{
					Cell: st.cell, Stream: def.name, Sequence: seq, QoS: qos, Topic: st.plan.topics[index%len(st.plan.topics)],
					SentNanos: m.sent, AckMicros: m.ackMicros, FirstCopyMicro: m.firstMicro, Expected: expected,
					Received: received, Acked: acked, Interrupted: m.flags&mfInterrupted != 0, Outcome: outcome,
				})
			}
		}
		st.mu.Unlock()
	}
	return result
}

func hashMessage(cell, stream int, seq uint64) uint64 {
	state := uint64(cell)*0x9E3779B97F4A7C15 ^ uint64(stream)<<56 ^ seq
	return splitmix64(&state)
}
