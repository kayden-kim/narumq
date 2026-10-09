package auditlog

import (
	"context"
	"sync"
	"testing"

	portsaudit "narumq/internal/application/ports/audit"
)

// Traceability: SEC-10/SEC-11; docs/specs/tenancy-and-security.md §8.
func TestFileAuditReaderConcurrentPublishSnapshots(t *testing.T) {
	publisher := openAuditTestPublisher(t, t.TempDir(), 400)
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for index := 1; index <= 30; index++ {
			if err := publisher.Publish(auditLogTestEvent(index)); err != nil {
				t.Errorf("Publish(%d): %v", index, err)
				return
			}
		}
	}()
	for iteration := range 100 {
		page, err := publisher.ReadAuditRecords(context.Background(), portsaudit.AuditRecordQueryDTO{Limit: 100})
		if err != nil {
			t.Fatalf("query %d: %v", iteration, err)
		}
		for index := 1; index < len(page.Records); index++ {
			if page.Records[index-1].Seq <= page.Records[index].Seq {
				t.Fatalf("page is not newest-first: %+v", page.Records)
			}
		}
	}
	writers.Wait()
}
