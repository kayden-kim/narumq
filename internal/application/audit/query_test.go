package audit

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	portsaudit "narumq/internal/application/ports/audit"
)

type queryReaderStub struct {
	query portsaudit.AuditRecordQueryDTO
	page  portsaudit.AuditRecordPageDTO
	err   error
}

func (reader *queryReaderStub) ReadAuditRecords(_ context.Context, query portsaudit.AuditRecordQueryDTO) (portsaudit.AuditRecordPageDTO, error) {
	reader.query = query
	return reader.page, reader.err
}

// Scenarios: management-api-and-observability.md:MGT-SCN-019
func TestAuditQueryValidationAndCursorPages(t *testing.T) {
	reader := &queryReaderStub{page: portsaudit.AuditRecordPageDTO{Records: []portsaudit.AuditRecordDTO{{Seq: 8}}, HasMore: true}}
	service, err := NewQueryService(reader)
	if err != nil {
		t.Fatal(err)
	}
	got, err := service.Query(context.Background(), QueryCommandDTO{})
	if err != nil {
		t.Fatal(err)
	}
	if reader.query.Limit != 50 || !got.HasMore || got.NextCursor == nil {
		t.Fatalf("query/result = %+v / %+v", reader.query, got)
	}
	if decoded, err := base64.RawURLEncoding.DecodeString(*got.NextCursor); err != nil || string(decoded) != "seq:8" {
		t.Fatalf("cursor=%q decoded=%q err=%v", *got.NextCursor, decoded, err)
	}
	for _, limit := range []int{1, 1000} {
		reader.page = portsaudit.AuditRecordPageDTO{}
		if _, err := service.Query(context.Background(), QueryCommandDTO{Limit: limit}); err != nil {
			t.Errorf("limit %d: %v", limit, err)
		}
	}
	for _, cursor := range []string{"%%%", base64.RawURLEncoding.EncodeToString([]byte("offset:1")), base64.RawURLEncoding.EncodeToString([]byte("seq:0")), base64.RawURLEncoding.EncodeToString([]byte("seq:18446744073709551616"))} {
		if _, err := service.Query(context.Background(), QueryCommandDTO{Cursor: cursor}); !errors.Is(err, ErrInvalidQuery) {
			t.Errorf("cursor %q error=%v", cursor, err)
		}
	}
	invalid := []QueryCommandDTO{
		{Limit: 1001},
		{Result: "NOPE"},
		{ActorSubject: string(make([]rune, 257))},
		{Action: string(make([]rune, 129))},
		{TargetType: string(make([]rune, 65))},
		{TargetID: string(make([]rune, 513))},
		{RequestID: string(make([]rune, 129))},
	}
	for index, command := range invalid {
		if _, err := service.Query(context.Background(), command); !errors.Is(err, ErrInvalidQuery) {
			t.Errorf("invalid query %d error=%v", index, err)
		}
	}
	reader.page = portsaudit.AuditRecordPageDTO{Records: []portsaudit.AuditRecordDTO{{Seq: 9}}}
	got, err = service.Query(context.Background(), QueryCommandDTO{Limit: 1})
	if err != nil || got.NextCursor != nil || got.HasMore {
		t.Fatalf("terminal page=%+v err=%v", got, err)
	}
}

// Scenarios: management-api-and-observability.md:MGT-SCN-019
func TestAuditQueryRequiresReader(t *testing.T) {
	if _, err := NewQueryService(nil); err == nil || err.Error() != "audit record reader is required" {
		t.Fatalf("NewQueryService(nil) error=%v", err)
	}
}
