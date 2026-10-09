package audit

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	portsaudit "narumq/internal/application/ports/audit"
)

// ErrInvalidQuery indicates that caller-supplied audit query parameters are invalid.
var ErrInvalidQuery = errors.New("invalid audit query")

// QueryCommandDTO selects node-local audit records and defines their page.
type QueryCommandDTO struct {
	Cursor                                                        string
	Limit                                                         int
	ActorSubject, Action, TargetType, TargetID, RequestID, Result string
	From, To                                                      time.Time
}

// QueryResultDTO is one query page.
type QueryResultDTO struct {
	Records    []portsaudit.AuditRecordDTO
	NextCursor *string
	HasMore    bool
}

// QueryService validates queries and delegates durable reads.
type QueryService struct{ reader portsaudit.AuditRecordReader }

// NewQueryService creates a query service backed by a durable record reader.
func NewQueryService(reader portsaudit.AuditRecordReader) (*QueryService, error) {
	if reader == nil || isNilDependency(reader) {
		return nil, errors.New("audit record reader is required")
	}
	return &QueryService{reader: reader}, nil
}

// Query validates the command and reads a newest-first page of matching records.
func (service *QueryService) Query(ctx context.Context, command QueryCommandDTO) (QueryResultDTO, error) {
	limit := command.Limit
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 1000 {
		return QueryResultDTO{}, invalidAuditQuery("limit must be between 1 and 1000")
	}
	for _, item := range []struct {
		name, value string
		max         int
	}{
		{"actorSubject", command.ActorSubject, 256}, {"action", command.Action, 128}, {"targetType", command.TargetType, 64},
		{"targetId", command.TargetID, 512}, {"requestId", command.RequestID, 128},
	} {
		if utf8.RuneCountInString(item.value) > item.max {
			return QueryResultDTO{}, invalidAuditQuery(item.name + " exceeds maximum length")
		}
	}
	result := portsaudit.AuditResult(command.Result)
	if result != "" && result != portsaudit.AuditResultAllowed && result != portsaudit.AuditResultDenied && result != portsaudit.AuditResultFailed {
		return QueryResultDTO{}, invalidAuditQuery("result is invalid")
	}
	beforeSeq, err := decodeAuditCursor(command.Cursor)
	if err != nil {
		return QueryResultDTO{}, err
	}
	page, err := service.reader.ReadAuditRecords(ctx, portsaudit.AuditRecordQueryDTO{
		BeforeSeq: beforeSeq, Limit: limit, ActorSubject: command.ActorSubject, Action: command.Action,
		TargetType: command.TargetType, TargetID: command.TargetID, RequestID: command.RequestID,
		Result: result, From: command.From, To: command.To,
	})
	if err != nil {
		return QueryResultDTO{}, err
	}
	resultDTO := QueryResultDTO{Records: page.Records, HasMore: page.HasMore}
	if page.HasMore && len(page.Records) > 0 {
		cursor := encodeAuditCursor(page.Records[len(page.Records)-1].Seq)
		resultDTO.NextCursor = &cursor
	}
	return resultDTO, nil
}

func invalidAuditQuery(reason string) error { return fmt.Errorf("%w: %s", ErrInvalidQuery, reason) }

func decodeAuditCursor(cursor string) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(cursor)
	if err != nil {
		return 0, invalidAuditQuery("cursor is invalid")
	}
	value := string(decoded)
	if !strings.HasPrefix(value, "seq:") {
		return 0, invalidAuditQuery("cursor is invalid")
	}
	digits := strings.TrimPrefix(value, "seq:")
	if digits == "" {
		return 0, invalidAuditQuery("cursor is invalid")
	}
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return 0, invalidAuditQuery("cursor is invalid")
		}
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || seq == 0 {
		return 0, invalidAuditQuery("cursor is invalid")
	}
	return seq, nil
}

func encodeAuditCursor(seq uint64) string {
	return base64.RawURLEncoding.EncodeToString([]byte("seq:" + strconv.FormatUint(seq, 10)))
}
