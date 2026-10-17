package auditlog

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	portsaudit "narumq/internal/application/ports/audit"
)

// Traceability: NFR-17; docs/specs/tenancy-and-security.md §8.
// Scenarios: tenancy-and-security.md:SEC-SCN-051
func TestAuditBufferPreservesSevenDaysAcrossRotationAndRestart(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 400)
	now := time.Now().UTC().Truncate(time.Second)
	oldest := now.Add(-8 * 24 * time.Hour)
	const eventCount = 9
	for index := range eventCount {
		event := auditLogTestEvent(index + 1)
		event.OccurredAt = oldest.Add(time.Duration(index) * 24 * time.Hour)
		if err := publisher.Publish(event); err != nil {
			t.Fatalf("Publish(day %d): %v", index, err)
		}
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	rotations, err := listAuditRotationFiles(directory)
	if err != nil || len(rotations) < 2 {
		t.Fatalf("rotation count=%d err=%v, want at least 2", len(rotations), err)
	}
	// Simulate old disk segments as well as old event timestamps. Reopening and
	// subsequent rotation must not prune the preceding seven days from the buffer.
	for _, rotation := range rotations {
		if err := os.Chtimes(rotation.path, oldest, oldest); err != nil {
			t.Fatal(err)
		}
	}
	publisher = openAuditTestPublisher(t, directory, 400)
	next := auditLogTestEvent(eventCount + 1)
	next.OccurredAt = now.Add(time.Second)
	if err := publisher.Publish(next); err != nil {
		t.Fatal(err)
	}
	page, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: eventCount + 1})
	if err != nil {
		t.Fatal(err)
	}
	if page.HasMore || len(page.Records) != eventCount+1 {
		t.Fatalf("retained records=%d hasMore=%t, want all %d", len(page.Records), page.HasMore, eventCount+1)
	}
	for index, record := range page.Records {
		wantSeq := uint64(eventCount + 1 - index)
		if record.Seq != wantSeq {
			t.Fatalf("record %d seq=%d, want %d", index, record.Seq, wantSeq)
		}
		var event auditEventWire
		if err := json.Unmarshal(record.EventJSON, &event); err != nil {
			t.Fatal(err)
		}
		wantTime := oldest.Add(time.Duration(wantSeq-1) * 24 * time.Hour)
		if wantSeq == eventCount+1 {
			wantTime = next.OccurredAt
		}
		if event.OccurredAt != wantTime.Format(time.RFC3339Nano) {
			t.Fatalf("record %d occurredAt=%s, want %s", record.Seq, event.OccurredAt, wantTime.Format(time.RFC3339Nano))
		}
	}
}
