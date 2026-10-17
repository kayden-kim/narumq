package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"narumq/tests/benchmark/internal/jsonl"
)

// TestClassifyQuotaRejectionUsesOnlyWhatThePacketSays checks that the class comes from the
// Reason String or the packet type and is never guessed.
// Scenarios: performance-and-verification.md:PRF-SCN-117
func TestClassifyQuotaRejectionUsesOnlyWhatThePacketSays(t *testing.T) {
	cases := []struct {
		name         string
		packet       quotaPacket
		reason       byte
		reasonString string
		wantClass    quotaClass
		wantCause    string
	}{
		{"puback tenant payload", quotaPacketPubAck, 0x97, "tenant_limit:payload_size", quotaTenantLimit, "payload_size"},
		{"pubrec tenant rate", quotaPacketPubRec, 0x97, "tenant_limit:message_rate", quotaTenantLimit, "message_rate"},
		{"puback tenant quota", quotaPacketPubAck, 0x97, "tenant_limit:tenant_quota", quotaTenantLimit, "tenant_quota"},
		{"puback commit lag", quotaPacketPubAck, 0x97, "server_backpressure:commit_lag", quotaServerBackpressure, "commit_lag"},
		{"pubrec disk", quotaPacketPubRec, 0x97, "server_backpressure:disk_free", quotaServerBackpressure, "disk_free"},
		{"puback retained", quotaPacketPubAck, 0x97, "server_backpressure:retained_capacity", quotaServerBackpressure, "retained_capacity"},
		{"connack quota", quotaPacketConnAck, 0x97, "", quotaTenantLimit, "connections_or_sessions"},
		{"puback without reason string", quotaPacketPubAck, 0x97, "", quotaUnclassified, ""},
		{"pubrec without reason string", quotaPacketPubRec, 0x97, "", quotaUnclassified, ""},
		{"no separator", quotaPacketPubAck, 0x97, "tenant_limit", quotaUnclassified, ""},
		{"empty cause", quotaPacketPubAck, 0x97, "server_backpressure:", quotaUnclassified, ""},
		{"unknown class", quotaPacketPubAck, 0x97, "other:commit_lag", quotaUnclassified, ""},
		{"unknown tenant cause", quotaPacketPubAck, 0x97, "tenant_limit:unknown", quotaUnclassified, ""},
		{"unknown server cause", quotaPacketPubAck, 0x97, "server_backpressure:unknown", quotaUnclassified, ""},
		{"extra separator", quotaPacketPubAck, 0x97, "server_backpressure:commit_lag:extra", quotaUnclassified, ""},
		{"free text", quotaPacketPubAck, 0x97, "quota exceeded", quotaUnclassified, ""},
		{"class is case sensitive", quotaPacketPubAck, 0x97, "Tenant_Limit:payload_size", quotaUnclassified, ""},
		{"connack rate limit", quotaPacketConnAck, 0x9F, "", quotaUnclassified, ""},
		{"puback rate limit", quotaPacketPubAck, 0x9F, "tenant_limit:message_rate", quotaUnclassified, ""},
		{"suback quota", quotaPacketSubAck, 0x97, "", quotaUnclassified, ""},
		{"suback quota with reason string", quotaPacketSubAck, 0x97, "tenant_limit:subscriptions", quotaUnclassified, ""},
	}
	for _, tc := range cases {
		class, cause := classifyQuotaRejection(tc.packet, tc.reason, tc.reasonString)
		if class != tc.wantClass || cause != tc.wantCause {
			t.Errorf("%s: got (%s, %q), want (%s, %q)", tc.name, class, cause, tc.wantClass, tc.wantCause)
		}
	}
	for reason, want := range map[byte]bool{0x97: true, 0x9F: true, 0x00: false, 0x80: false, 0x96: false} {
		if isQuotaRejection(reason) != want {
			t.Errorf("isQuotaRejection(0x%02X) = %t, want %t", reason, !want, want)
		}
	}
}

// TestVerdictCountsQuotaClassesSeparately checks the verdict of each class and their mix.
// Scenarios: performance-and-verification.md:PRF-SCN-118
func TestVerdictCountsQuotaClassesSeparately(t *testing.T) {
	cases := []struct {
		name        string
		mutate      func(*verdictInput)
		want        string
		wantReason  string
		wantFailing []string
	}{
		{"none", func(*verdictInput) {}, verdictPass, "", nil},
		{"tenant limit", func(in *verdictInput) { in.TenantLimitRejections = 3 }, verdictInvalid, "tenant limit rejection observed 3 times", nil},
		{"server backpressure", func(in *verdictInput) { in.ServerBackpressureRejections = 2 }, verdictFail, "server backpressure rejection observed 2 times", []string{"server backpressure rejection observed 2 times"}},
		{"unclassified", func(in *verdictInput) { in.UnclassifiedRejections = 1 }, verdictFail, "unclassified quota rejection observed 1 times", []string{"unclassified quota rejection observed 1 times"}},
		{"tenant limit and server backpressure", func(in *verdictInput) {
			in.TenantLimitRejections, in.ServerBackpressureRejections, in.UnclassifiedRejections = 1, 2, 4
		}, verdictInvalid, "tenant limit rejection observed 1 times", []string{"server backpressure rejection observed 2 times", "unclassified quota rejection observed 4 times"}},
	}
	for _, tc := range cases {
		in := passingInput()
		tc.mutate(&in)
		got := computeVerdict(in)
		if got.Verdict != tc.want {
			t.Errorf("%s: verdict %s (%s), want %s", tc.name, got.Verdict, joined(got), tc.want)
			continue
		}
		if !strings.Contains(joined(got), tc.wantReason) {
			t.Errorf("%s: reasons %q miss %q", tc.name, got.Reasons, tc.wantReason)
		}
		if len(got.FailConditions) != len(tc.wantFailing) {
			t.Errorf("%s: fail conditions %q, want %d entries", tc.name, got.FailConditions, len(tc.wantFailing))
			continue
		}
		for i, want := range tc.wantFailing {
			if !strings.Contains(got.FailConditions[i], want) {
				t.Errorf("%s: fail condition %d %q misses %q", tc.name, i, got.FailConditions[i], want)
			}
		}
	}
	// Invalid keeps the other class counts as evidence but does not list them as invalid reasons.
	in := passingInput()
	in.TenantLimitRejections, in.ServerBackpressureRejections = 1, 2
	if got := computeVerdict(in); strings.Contains(joined(got), "server backpressure") {
		t.Errorf("invalid reasons must not list server backpressure: %q", got.Reasons)
	}
}

func readQuotaRecords(t *testing.T, path string) []quotaRejectionRecord {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var records []quotaRejectionRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record quotaRejectionRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("%s: %v", scanner.Text(), err)
		}
		records = append(records, record)
	}
	return records
}

// TestQuotaRejectionsAreRecordedPerWindowAndNeverDropped checks the per-rejection artifact, the
// per-window counts and that no window is exempt from the verdict.
// Scenarios: performance-and-verification.md:PRF-SCN-119
func TestQuotaRejectionsAreRecordedPerWindowAndNeverDropped(t *testing.T) {
	col := newTestCollector(t)
	path := filepath.Join(t.TempDir(), "quota-rejections.jsonl")
	writer, err := jsonl.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	env := &stepEnv{col: col, rejections: writer}
	type observed struct {
		packet       quotaPacket
		reason       byte
		reasonString string
		class        string
		cause        string
	}
	observations := [numWindows]observed{
		winSetup:     {quotaPacketConnAck, 0x97, "", "tenant_limit", "connections_or_sessions"},
		winWarmup:    {quotaPacketPubAck, 0x97, "server_backpressure:commit_lag", "server_backpressure", "commit_lag"},
		winMeasureN:  {quotaPacketPubRec, 0x97, "tenant_limit:message_rate", "tenant_limit", "message_rate"},
		winFailure:   {quotaPacketPubAck, 0x97, "", "unclassified", ""},
		winMeasureN1: {quotaPacketSubAck, 0x97, "", "unclassified", ""},
		winRestore:   {quotaPacketConnAck, 0x9F, "", "unclassified", ""},
		winCleanup:   {quotaPacketPubAck, 0x97, "server_backpressure:disk_free", "server_backpressure", "disk_free"},
	}
	for window := range numWindows {
		spec := clientSpec{id: "client-" + window.String(), tenant: "tenant-" + window.String(), cell: 0, stream: streamAlert}
		col.setWindow(window)
		o := observations[window]
		switch o.packet {
		case quotaPacketPubAck, quotaPacketPubRec:
			seq, _ := col.begin(spec.cell, spec.stream)
			client := &sessClient{env: env, spec: spec}
			client.finishAck(&pendingPublish{seq: seq}, o.packet, o.reason, o.reasonString)
		default:
			env.rejectQuota(spec, o.packet, o.reason, o.reasonString)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	records := readQuotaRecords(t, path)
	if len(records) != int(numWindows) {
		t.Fatalf("%d records, want %d", len(records), numWindows)
	}
	for window, record := range records {
		o := observations[window]
		if record.Phase != windowID(window).String() || record.ClientID != "client-"+record.Phase || record.Tenant != "tenant-"+record.Phase ||
			record.Packet != string(o.packet) || record.ReasonCode != int(o.reason) || record.ReasonString != o.reasonString ||
			record.Classification != o.class || record.Cause != o.cause || record.At == "" {
			t.Errorf("record %d = %+v, want %+v", window, record, o)
		}
	}

	summary := col.quota.summary()
	wantTotal := quotaClassCounts{TenantLimit: 2, ServerBackpressure: 2, Unclassified: 3}
	if summary.Total != wantTotal || len(summary.ByWindow) != int(numWindows) {
		t.Fatalf("summary = %+v, want total %+v in every window", summary, wantTotal)
	}
	for window := range numWindows {
		counts := summary.ByWindow[window.String()]
		if counts.TenantLimit+counts.ServerBackpressure+counts.Unclassified != 1 {
			t.Errorf("window %s counts = %+v, want exactly one rejection", window, counts)
		}
	}
	encoded, err := json.Marshal(stepSummary{QuotaRejections: summary})
	if err != nil || !strings.Contains(string(encoded), `"quotaRejections":{"total":{"tenantLimit":2,"serverBackpressure":2,"unclassified":3}`) {
		t.Fatalf("encoded summary = %s (%v)", encoded, err)
	}

	// A rejection in the warmup window alone still keeps the step from passing.
	for _, window := range []windowID{winSetup, winWarmup, winCleanup} {
		fresh := &collector{}
		fresh.quota.add(window, quotaServerBackpressure)
		in := passingInput()
		in.ServerBackpressureRejections = fresh.quota.total(quotaServerBackpressure)
		if got := computeVerdict(in); got.Verdict != verdictFail {
			t.Errorf("%s rejection: verdict %s, want fail", window, got.Verdict)
		}
	}

	// A refused publish is neither an ACK nor a protocol error.
	if errors := col.counters().protocolErrors.Load(); errors != 0 {
		t.Errorf("protocol errors = %d, want 0", errors)
	}
	gaps := col.evaluate(0, func(messageRecord) {})
	if gaps.Gaps != [3]uint64{} {
		t.Fatalf("rejected publishes became ACKed gaps: %+v", gaps)
	}
}

// Scenarios: performance-and-verification.md:PRF-SCN-119
func TestQuotaRejectionArtifactWriteFailureIsPreserved(t *testing.T) {
	writer, err := jsonl.Open(filepath.Join(t.TempDir(), "quota-rejections.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	env := &stepEnv{col: newTestCollector(t), rejections: writer}
	env.rejectQuota(clientSpec{id: "publisher", tenant: "tenant"}, quotaPacketPubAck, 0x97, "")
	first := env.rejectionErr
	if first == nil {
		t.Fatal("artifact write failure was discarded")
	}
	env.rejectQuota(clientSpec{}, quotaPacketPubAck, 0x97, "")
	if env.rejectionErr != first || env.col.quota.total(quotaUnclassified) != 2 {
		t.Fatal("first artifact failure or refusal count was lost")
	}
}
