package auditlog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	portsaudit "narumq/internal/application/ports/audit"
)

// Traceability: SEC-10/SEC-11; docs/specs/tenancy-and-security.md §8.
// Scenarios: management-api-and-observability.md:MGT-SCN-019, tenancy-and-security.md:SEC-SCN-033
func TestFileAuditReaderPagesAcrossRotationsAndPreservesStoredEvent(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 400)
	publishAuditTestEvents(t, publisher, 8)
	rotations, err := listAuditRotationFiles(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotations) < 2 {
		t.Fatalf("rotation count=%d, want at least 2", len(rotations))
	}
	first, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !first.HasMore || len(first.Records) != 3 || first.Records[0].Seq != 8 || first.Records[2].Seq != 6 {
		t.Fatalf("first page=%+v", first)
	}
	second, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 3, BeforeSeq: first.Records[2].Seq})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 3 || second.Records[0].Seq != 5 || second.Records[2].Seq != 3 {
		t.Fatalf("second page=%+v", second)
	}
	last, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 3, BeforeSeq: second.Records[2].Seq})
	if err != nil {
		t.Fatal(err)
	}
	if len(last.Records) != 2 || last.HasMore || last.Records[0].Seq != 2 || last.Records[1].Seq != 1 {
		t.Fatalf("last page=%+v", last)
	}
	stored, err := os.ReadFile(filepath.Join(directory, activeFileName))
	if err != nil {
		t.Fatal(err)
	}
	active, err := scanAuditData(stored, activeFileName, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) > 0 {
		all, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if string(all.Records[0].EventJSON) != string(active[len(active)-1].Event) {
			t.Fatalf("event bytes changed: %s != %s", all.Records[0].EventJSON, active[len(active)-1].Event)
		}
	}
	filtered, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 10, ActorSubject: "management-bearer-token", Action: "node.drain.start", TargetType: "node", TargetID: "3", RequestID: "request-3", Result: portsaudit.AuditResultAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Records) != 1 || filtered.Records[0].Seq != 3 {
		t.Fatalf("filtered=%+v", filtered)
	}
	ranged, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 10, From: auditLogTestEvent(3).OccurredAt, To: auditLogTestEvent(5).OccurredAt})
	if err != nil {
		t.Fatal(err)
	}
	if len(ranged.Records) != 2 || ranged.Records[0].Seq != 4 || ranged.Records[1].Seq != 3 {
		t.Fatalf("inclusive/exclusive time range=%+v", ranged)
	}
}

// Traceability: SEC-10/SEC-11; docs/specs/tenancy-and-security.md §8.
// Scenarios: management-api-and-observability.md:MGT-SCN-019, tenancy-and-security.md:SEC-SCN-033
func TestFileAuditReaderHandlesRetentionCorruptionAndClose(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 400)
	publishAuditTestEvents(t, publisher, 8)
	rotations, err := listAuditRotationFiles(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotations) < 2 {
		t.Fatalf("rotation count=%d", len(rotations))
	}
	if err := os.Remove(rotations[0].path); err != nil {
		t.Fatal(err)
	}
	page, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 100})
	if err != nil {
		t.Fatalf("retained prefix read: %v", err)
	}
	if len(page.Records) == 0 {
		t.Fatal("retention returned no available records")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := publisher.ReadAuditRecords(canceled, portsaudit.AuditRecordQueryDTO{Limit: 1}); !errors.Is(err, portsaudit.ErrAuditRecordsUnavailable) {
		t.Fatalf("canceled reader error=%v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 1}); !errors.Is(err, portsaudit.ErrAuditRecordsUnavailable) {
		t.Fatalf("closed reader error=%v", err)
	}

	badDirectory := t.TempDir()
	bad := openAuditTestPublisher(t, badDirectory, 400)
	publishAuditTestEvents(t, bad, 8)
	files, err := listAuditRotationFiles(badDirectory)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(files[len(files)-1].path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 1
	if err := os.WriteFile(files[len(files)-1].path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := bad.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 100}); !errors.Is(err, portsaudit.ErrAuditRecordsUnavailable) {
		t.Fatalf("corruption error=%v", err)
	}
}

// Traceability: SEC-10/SEC-11; docs/specs/tenancy-and-security.md §8.
// Scenarios: management-api-and-observability.md:MGT-SCN-019, tenancy-and-security.md:SEC-SCN-033
func TestFileAuditReaderRejectsInvalidStoredTimestamp(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1<<20)
	publishAuditTestEvents(t, publisher, 1)
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, activeFileName)
	records := readAuditRecords(t, path)
	event := bytes.Replace(records[0].Event, []byte(`"occurredAt":"2026-10-08T12:00:01Z"`), []byte(`"occurredAt":"not-a-time"`), 1)
	if bytes.Equal(event, records[0].Event) {
		t.Fatal("test event timestamp was not replaced")
	}
	line := makeAuditLine(records[0].Seq, records[0].PrevHash, event, calculateAuditHash(records[0].Seq, records[0].PrevHash, event))
	if err := os.WriteFile(path, line, 0o600); err != nil {
		t.Fatal(err)
	}
	publisher = openAuditTestPublisher(t, directory, 1<<20)
	_, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 10})
	if !errors.Is(err, portsaudit.ErrAuditRecordsUnavailable) {
		t.Fatalf("invalid stored timestamp error = %v, want unavailable", err)
	}
}

// Traceability: SEC-10/SEC-11; docs/specs/tenancy-and-security.md §8.
// Scenarios: management-api-and-observability.md:MGT-SCN-019, tenancy-and-security.md:SEC-SCN-033
func TestFileAuditReaderRejectsBrokenCrossFileChainLink(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1)
	publishAuditTestEvents(t, publisher, 2)
	rotations, err := listAuditRotationFiles(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(rotations) != 1 {
		t.Fatalf("rotation count = %d, want 1", len(rotations))
	}
	records := readAuditRecords(t, rotations[0].path)
	wrongPreviousHash := string(bytes.Repeat([]byte("1"), 64))
	wrongHash := calculateAuditHash(records[0].Seq, wrongPreviousHash, records[0].Event)
	line := makeAuditLine(records[0].Seq, wrongPreviousHash, records[0].Event, wrongHash)
	if err := os.WriteFile(rotations[0].path, line, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 2})
	if !errors.Is(err, portsaudit.ErrAuditRecordsUnavailable) {
		t.Fatalf("broken chain error = %v, want unavailable", err)
	}
}
