package main

import (
	"strings"
	"testing"
	"time"
)

const (
	streamAlert   = 1 // a_alert: QoS 1, persistent, four broadcast subscribers
	streamCommand = 2 // a_command: QoS 2, persistent, one exact subscriber per topic
	streamJob     = 3 // a_job: QoS 1, shared group of four clean-session subscribers
)

func newTestCollector(t *testing.T) *collector {
	t.Helper()
	p := testProfile(t)
	return newCollector(p, 1, []*cellPlan{buildCell(p, 0)})
}

func sendAcked(c *collector, stream int) uint64 {
	seq, _ := c.begin(0, stream)
	c.ack(0, stream, seq)
	return seq
}

func TestEvaluateCountsAckedGapButOnlyRecordsQoS1Duplicate(t *testing.T) {
	c := newTestCollector(t)
	full := sendAcked(c, streamAlert)
	for sub := range 4 {
		c.deliver(0, streamAlert, sub, full)
	}
	missingOne := sendAcked(c, streamAlert)
	for sub := range 3 {
		c.deliver(0, streamAlert, sub, missingOne)
	}
	duplicated := sendAcked(c, streamAlert)
	for sub := range 4 {
		c.deliver(0, streamAlert, sub, duplicated)
	}
	if kind := c.deliver(0, streamAlert, 0, duplicated); kind != deliverDuplicate {
		t.Fatalf("second copy kind = %v, want duplicate", kind)
	}
	// An unacknowledged, undelivered publish is neither a gap nor a failure.
	c.begin(0, streamAlert)

	result := c.evaluate(time.Second, nil)
	if result.Gaps[1] != 1 || result.CleanSessionLoss[1].Count != 0 {
		t.Fatalf("gaps %v cleanSessionLoss %v, want exactly one QoS1 gap", result.Gaps, result.CleanSessionLoss)
	}
	if got := c.duplicates[1].Load(); got != 1 {
		t.Fatalf("QoS1 duplicates = %d, want 1", got)
	}
	if result.Acked[1] != 3 || result.Sent[1] != 4 {
		t.Fatalf("acked %v sent %v", result.Acked, result.Sent)
	}
}

func TestEvaluateCountsQoS2DuplicateAndUnexpectedRecipient(t *testing.T) {
	c := newTestCollector(t)
	seq := sendAcked(c, streamCommand)
	// Sequence 1 goes to topic dev00, which only subscriber 0 filters for.
	if kind := c.deliver(0, streamCommand, 1, seq); kind != deliverUnexpected {
		t.Fatalf("delivery to a non-matching subscriber = %v, want unexpected", kind)
	}
	c.deliver(0, streamCommand, 0, seq)
	c.deliver(0, streamCommand, 0, seq)
	if got := c.duplicates[2].Load(); got != 1 {
		t.Fatalf("QoS2 duplicates = %d, want 1", got)
	}
	if got := c.unexpected.Load(); got != 1 {
		t.Fatalf("unexpected deliveries = %d, want 1", got)
	}
	if result := c.evaluate(time.Second, nil); result.Gaps[2] != 0 {
		t.Fatalf("QoS2 gaps = %v, want none", result.Gaps)
	}
}

func sendAckedAt(c *collector, stream int, sent time.Duration) {
	seq := sendAcked(c, stream)
	st := c.stream(0, stream)
	st.mu.Lock()
	st.msgs[seq-1].sent = int64(sent)
	st.mu.Unlock()
}

func TestCleanSessionLossNeedsADisconnectAfterThePublishWhateverItsAge(t *testing.T) {
	c := newTestCollector(t)
	// The job group is shared among four clean-session subscribers. Nobody received either message.
	// Message A is followed by a connection loss of member 0 30 s later; message B is published
	// after that outage and every member stays connected.
	sendAckedAt(c, streamJob, 1*time.Second)
	c.subscriberDownAt(0, streamJob, 0, int64(31*time.Second))
	c.subscriberUpAt(0, streamJob, 0, int64(32*time.Second))
	sendAckedAt(c, streamJob, 40*time.Second)
	result := c.evaluate(time.Minute, nil)
	if result.Gaps[1] != 1 {
		t.Fatalf("gaps %v, want 1 (message B: no connection loss followed it)", result.Gaps)
	}
	if loss := result.CleanSessionLoss[1]; loss.Count != 1 || loss.MaxMillis != 30000 {
		t.Fatalf("cleanSessionLoss %+v, want count 1 and max 30000 ms (message A)", loss)
	}
}

func TestPersistentSubscriberIsNeverExcused(t *testing.T) {
	c := newTestCollector(t)
	// The persistent alert subscriber loses its connection right after the publish and misses the message.
	sendAckedAt(c, streamAlert, 1*time.Second)
	c.subscriberDownAt(0, streamAlert, 1, int64(1*time.Second+10*time.Millisecond))
	result := c.evaluate(time.Second, nil)
	if result.Gaps[1] != 4 || result.CleanSessionLoss[1].Count != 0 || result.NotExpected[1] != 0 {
		t.Fatalf("gaps %v loss %v notExpected %v, want 4 gaps (every persistent recipient missed it)",
			result.Gaps, result.CleanSessionLoss, result.NotExpected)
	}
}

func TestPublishWhileCleanSubscribersAreDisconnectedIsNotExpected(t *testing.T) {
	c := newTestCollector(t)
	for member := range 4 {
		c.subscriberDownAt(0, streamJob, member, int64(1*time.Second))
		c.subscriberUpAt(0, streamJob, member, int64(5*time.Second))
	}
	sendAckedAt(c, streamJob, 2*time.Second)
	// Published after the new SUBACK, nobody received it, nobody disconnected: a gap.
	sendAckedAt(c, streamJob, 6*time.Second)
	result := c.evaluate(time.Second, nil)
	if result.NotExpected[1] != 1 || result.Gaps[1] != 1 {
		t.Fatalf("notExpected %v gaps %v, want 1 each", result.NotExpected, result.Gaps)
	}
}

func TestDeliveryLatencyIsAttributedToTheReceiptWindow(t *testing.T) {
	c := newTestCollector(t)
	seq, _ := c.begin(0, streamAlert)
	c.ack(0, streamAlert, seq)
	c.setWindow(winMeasureN)
	c.deliver(0, streamAlert, 0, seq)
	c.setWindow(winMeasureN1)
	c.deliver(0, streamAlert, 1, seq)
	c.setWindow(winFailure)
	c.deliver(0, streamAlert, 2, seq)
	if n, n1 := c.hist[0][1].Count(), c.hist[1][1].Count(); n != 1 || n1 != 1 {
		t.Fatalf("latency samples measureN=%d measureN1=%d, want 1 each (failure window is not measured)", n, n1)
	}
}

func TestCleanSessionLossAtLatencyLimitIsExcludedAndOlderIsOverLimit(t *testing.T) {
	const limit = 100 * time.Millisecond
	c := newTestCollector(t)
	// Shared clean-session group, nobody received either message. Message A is followed by a
	// connection loss exactly at the limit; message B 1 ms beyond it.
	sendAckedAt(c, streamJob, 1*time.Second)
	c.subscriberDownAt(0, streamJob, 0, int64(time.Second+limit))
	c.subscriberUpAt(0, streamJob, 0, int64(2*time.Second))
	sendAckedAt(c, streamJob, 10*time.Second)
	c.subscriberDownAt(0, streamJob, 1, int64(10*time.Second+limit+time.Millisecond))
	c.subscriberUpAt(0, streamJob, 1, int64(11*time.Second))
	result := c.evaluate(limit, nil)
	loss := result.CleanSessionLoss[1]
	if loss.Count != 2 || loss.OverLimit != 1 || loss.MaxMillis != 101 || result.Gaps[1] != 0 {
		t.Fatalf("loss %+v gaps %v, want 2 losses of which 1 over the limit, max 101 ms, no gaps", loss, result.Gaps)
	}
	in := passingInput()
	in.CleanSessionLossOverLimit, in.CleanSessionLossMaxMillis = lossOverLimit(result), lossMaxMillis(result)
	if got := computeVerdict(in); got.Verdict != verdictFail || !strings.Contains(joined(got), "clean-session loss older than latency limit") {
		t.Fatalf("verdict %s (%s), want fail for the loss older than the limit", got.Verdict, joined(got))
	}
	onlyAtLimit := newTestCollector(t)
	sendAckedAt(onlyAtLimit, streamJob, 1*time.Second)
	onlyAtLimit.subscriberDownAt(0, streamJob, 0, int64(time.Second+limit))
	if r := onlyAtLimit.evaluate(limit, nil); lossOverLimit(r) != 0 || r.CleanSessionLoss[1].Count != 1 {
		t.Fatalf("loss exactly at the limit: %+v, want excluded and not over limit", r.CleanSessionLoss[1])
	}
}

func TestQoS2PubrecThenDisconnectMissingIsCleanPublisherInterruptedNotGap(t *testing.T) {
	c := newTestCollector(t)
	// PUBREC received, publisher connection broke before PUBCOMP, delivery missing.
	interrupted := sendAcked(c, streamCommand)
	c.publishInterrupted(0, streamCommand, interrupted)
	// PUBCOMP received (no interruption), delivery missing: a gap.
	sendAcked(c, streamCommand)
	// PUBREC never arrived: unacked, not a gap.
	unacked, _ := c.begin(0, streamCommand)
	c.publishInterrupted(0, streamCommand, unacked)
	result := c.evaluate(time.Second, nil)
	if result.Gaps[2] != 1 || result.CleanPublisherInterrupted != 1 || result.AckedInterrupted[2] != 1 || result.Unacked[2] != 1 {
		t.Fatalf("gaps %v cleanPublisherInterrupted %d ackedInterrupted %v unacked %v, want 1 each",
			result.Gaps, result.CleanPublisherInterrupted, result.AckedInterrupted, result.Unacked)
	}
}
