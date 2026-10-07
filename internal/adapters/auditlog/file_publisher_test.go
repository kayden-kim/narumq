package auditlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	portsaudit "narumq/internal/application/ports/audit"
)

func auditLogTestEvent(index int) portsaudit.AuditEventDTO {
	return portsaudit.AuditEventDTO{
		SchemaVersion: "1.0",
		EventID:       fmt.Sprintf("event-%d", index),
		OccurredAt:    time.Date(2026, 10, 8, 12, 0, index, 0, time.UTC),
		RequestID:     fmt.Sprintf("request-%d", index),
		Producer: portsaudit.AuditProducerDTO{
			Component:  "BROKER",
			InstanceID: "cluster/1",
			Version:    "1.2.3",
		},
		Actor:  portsaudit.AuditActorDTO{Subject: "management-bearer-token"},
		Action: "node.drain.start",
		Target: portsaudit.AuditTargetDTO{Type: "node", ID: "3"},
		Result: portsaudit.AuditResultAllowed,
		Reason: "succeeded",
	}
}

func openAuditTestPublisher(t *testing.T, directory string, maxBytes int64) *FileAuditPublisher {
	t.Helper()
	publisher, err := OpenFileAuditPublisher(FileAuditPublisherConfig{Directory: directory, MaxFileBytes: maxBytes})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher() error = %v", err)
	}
	if err := publisher.Ready(); err != nil {
		t.Fatalf("publisher.Ready() error = %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })
	return publisher
}

func publishAuditTestEvents(t *testing.T, publisher *FileAuditPublisher, count int) {
	t.Helper()
	for index := 1; index <= count; index++ {
		if err := publisher.Publish(auditLogTestEvent(index)); err != nil {
			t.Fatalf("Publish(%d) error = %v", index, err)
		}
	}
}

func readAuditRecords(t *testing.T, path string) []auditLogRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	records, err := scanAuditData(data, filepath.Base(path), false)
	if err != nil {
		t.Fatalf("scan audit log: %v", err)
	}
	return records
}

func independentAuditHash(sequence uint64, previous string, event []byte) string {
	input := append([]byte(fmt.Sprintf("%d\n%s\n", sequence, previous)), event...)
	digest := sha256.Sum256(input)
	return hex.EncodeToString(digest[:])
}

func TestFileAuditPublisherPersistsAndRecoversHashChain(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1<<20)
	publishAuditTestEvents(t, publisher, 3)
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	path := filepath.Join(directory, activeFileName)
	records := readAuditRecords(t, path)
	if len(records) != 3 {
		t.Fatalf("record count = %d, want 3", len(records))
	}
	previous := genesisHash
	for index, record := range records {
		wantSeq := uint64(index + 1)
		if record.Seq != wantSeq || record.PrevHash != previous {
			t.Fatalf("record %d sequence/previous hash = %d/%s, want %d/%s", index+1, record.Seq, record.PrevHash, wantSeq, previous)
		}
		if want := independentAuditHash(record.Seq, record.PrevHash, record.Event); record.Hash != want {
			t.Fatalf("record %d hash = %s, want %s", index+1, record.Hash, want)
		}
		previous = record.Hash
	}
	var eventFields map[string]json.RawMessage
	if err := json.Unmarshal(records[0].Event, &eventFields); err != nil {
		t.Fatalf("decode event JSON: %v", err)
	}
	for _, omitted := range []string{"tenantId", "credentialId", "roles", "details", "policyVersion", "sourceIp"} {
		if _, exists := eventFields[omitted]; exists {
			t.Fatalf("empty optional field %q was written: %s", omitted, records[0].Event)
		}
	}

	publisher = openAuditTestPublisher(t, directory, 1<<20)
	if err := publisher.Publish(auditLogTestEvent(4)); err != nil {
		t.Fatalf("Publish(4) after reopen error = %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() after reopen error = %v", err)
	}
	records = readAuditRecords(t, path)
	if len(records) != 4 || records[3].Seq != 4 || records[3].PrevHash != records[2].Hash {
		t.Fatalf("reopened chain tail = %+v, want sequence 4 linked to sequence 3", records[len(records)-1])
	}
}

func TestFileAuditPublisherTruncatesTornActiveTail(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1<<20)
	publishAuditTestEvents(t, publisher, 2)
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	path := filepath.Join(directory, activeFileName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open active log: %v", err)
	}
	if _, err := file.Write([]byte(`{"seq":`)); err != nil {
		t.Fatalf("append torn bytes: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close active log: %v", err)
	}

	publisher = openAuditTestPublisher(t, directory, 1<<20)
	if err := publisher.Publish(auditLogTestEvent(3)); err != nil {
		t.Fatalf("Publish after torn-tail recovery error = %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() after recovery error = %v", err)
	}
	records := readAuditRecords(t, path)
	if len(records) != 3 || records[2].Seq != 3 {
		t.Fatalf("records after recovery = %+v, want three complete records", records)
	}
}

func TestFileAuditPublisherRejectsCorruptRecordAndStaysUnavailable(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1<<20)
	if err := publisher.Publish(auditLogTestEvent(1)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	path := filepath.Join(directory, activeFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read active log: %v", err)
	}
	corrupt := bytes.Replace(data, []byte(`"requestId":"request-1"`), []byte(`"requestId":"request-x"`), 1)
	if bytes.Equal(data, corrupt) {
		t.Fatal("corruption did not change the event")
	}
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatalf("write corrupt log: %v", err)
	}

	publisher, err = OpenFileAuditPublisher(FileAuditPublisherConfig{Directory: directory, MaxFileBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher() error = %v, want unavailable publisher", err)
	}
	if err := publisher.Ready(); err == nil || !strings.Contains(err.Error(), "audit.jsonl:1") {
		t.Fatalf("Ready() error = %v, want file and line", err)
	}
	if err := publisher.Publish(auditLogTestEvent(2)); err == nil {
		t.Fatal("Publish() succeeded after recovery validation failed")
	}
	_ = publisher.Close()
}

func TestFileAuditPublisherRotatesWithContinuousSequence(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1)
	publishAuditTestEvents(t, publisher, 3)
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	for index := 1; index <= 2; index++ {
		name := fmt.Sprintf("audit-%020d.jsonl", index)
		if _, err := os.Stat(filepath.Join(directory, name)); err != nil {
			t.Fatalf("stat rotation %s: %v", name, err)
		}
	}
	result, err := VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory() error = %v", err)
	}
	if result.Files != 3 || result.Records != 3 || result.FirstSeq != 1 || result.LastSeq != 3 {
		t.Fatalf("verification result = %+v, want three linked records across three files", result)
	}
}

func TestOpenFileAuditPublisherReportsIOMismatchAsUnavailable(t *testing.T) {
	parent := t.TempDir()
	filePath := filepath.Join(parent, "not-a-directory")
	if err := os.WriteFile(filePath, []byte("file"), 0o600); err != nil {
		t.Fatalf("create regular file: %v", err)
	}
	publisher, err := OpenFileAuditPublisher(FileAuditPublisherConfig{Directory: filePath, MaxFileBytes: 1024})
	if err != nil {
		t.Fatalf("OpenFileAuditPublisher() error = %v, want unavailable publisher", err)
	}
	if err := publisher.Ready(); err == nil || !strings.Contains(err.Error(), "open audit log") {
		t.Fatalf("Ready() error = %v, want open failure", err)
	}
	if err := publisher.Publish(auditLogTestEvent(1)); err == nil {
		t.Fatal("Publish() succeeded for an unavailable publisher")
	}
	_ = publisher.Close()
}

func TestOpenFileAuditPublisherRejectsInvalidConfiguration(t *testing.T) {
	for _, config := range []FileAuditPublisherConfig{{MaxFileBytes: 1}, {Directory: t.TempDir()}} {
		if publisher, err := OpenFileAuditPublisher(config); err == nil || publisher != nil {
			t.Fatalf("OpenFileAuditPublisher(%+v) = (%v, %v), want configuration error", config, publisher, err)
		}
	}
}

func TestVerifyDirectoryDetectsMissingRecordAndReportsTornActiveTail(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1<<20)
	publishAuditTestEvents(t, publisher, 3)
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	path := filepath.Join(directory, activeFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read active log: %v", err)
	}
	lines := bytes.Split(data, []byte{'\n'})
	withoutMiddle := append(append([]byte(nil), lines[0]...), '\n')
	withoutMiddle = append(withoutMiddle, lines[2]...)
	withoutMiddle = append(withoutMiddle, '\n')
	if err := os.WriteFile(path, withoutMiddle, 0o600); err != nil {
		t.Fatalf("write log with missing record: %v", err)
	}
	_, err = VerifyDirectory(directory)
	if err == nil || !strings.Contains(err.Error(), "audit.jsonl:2") {
		t.Fatalf("VerifyDirectory() error = %v, want missing-record error with file and line", err)
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("restore active log: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open active log: %v", err)
	}
	if _, err := file.Write([]byte(`{"seq":`)); err != nil {
		t.Fatalf("append torn bytes: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close active log: %v", err)
	}
	result, err := VerifyDirectory(directory)
	if err != nil || !result.TornTail || result.Records != 3 {
		t.Fatalf("VerifyDirectory() = (%+v, %v), want read-only torn-tail result", result, err)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || !bytes.HasSuffix(unchanged, []byte(`{"seq":`)) {
		t.Fatalf("VerifyDirectory modified torn tail: %v", err)
	}
}

func TestVerifyDirectoryAllowsDeletedOldestRotation(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1)
	publishAuditTestEvents(t, publisher, 3)
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	oldest := filepath.Join(directory, "audit-00000000000000000001.jsonl")
	if err := os.Remove(oldest); err != nil {
		t.Fatalf("remove collected oldest rotation: %v", err)
	}
	result, err := VerifyDirectory(directory)
	if err != nil {
		t.Fatalf("VerifyDirectory() after deleting oldest rotation error = %v", err)
	}
	if result.FirstSeq != 2 || result.LastSeq != 3 || result.Records != 2 {
		t.Fatalf("verification result = %+v, want retained chain beginning at sequence 2", result)
	}
}

func TestVerifyDirectoryRejectsTornRotationFile(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1)
	publishAuditTestEvents(t, publisher, 2)
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	rotation := filepath.Join(directory, "audit-00000000000000000001.jsonl")
	file, err := os.OpenFile(rotation, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open rotation: %v", err)
	}
	if _, err := file.Write([]byte(`{"seq":`)); err != nil {
		t.Fatalf("append torn rotation bytes: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close rotation: %v", err)
	}
	if _, err := VerifyDirectory(directory); err == nil || !strings.Contains(err.Error(), "audit-00000000000000000001.jsonl:") {
		t.Fatalf("VerifyDirectory() error = %v, want torn rotation error with file name", err)
	}
}

func TestFileAuditPublisherRotationCollisionMakesPublisherUnavailable(t *testing.T) {
	directory := t.TempDir()
	publisher := openAuditTestPublisher(t, directory, 1)
	if err := publisher.Publish(auditLogTestEvent(1)); err != nil {
		t.Fatalf("Publish(1) error = %v", err)
	}
	rotation := filepath.Join(directory, "audit-00000000000000000001.jsonl")
	if err := os.WriteFile(rotation, []byte("occupied"), 0o600); err != nil {
		t.Fatalf("create conflicting rotation file: %v", err)
	}
	if err := publisher.Publish(auditLogTestEvent(2)); err == nil {
		t.Fatal("Publish() succeeded despite rotation collision")
	}
	if err := publisher.Ready(); err == nil {
		t.Fatal("Ready() succeeded after rotation collision")
	}
}

func TestAuditHashFormattingIsLowercaseHex(t *testing.T) {
	if !validAuditHash(genesisHash) || validAuditHash(strings.Repeat("A", 64)) || validAuditHash("not-a-hash") {
		t.Fatal("hash validation did not enforce 64 lowercase hexadecimal characters")
	}
}
