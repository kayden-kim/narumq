package auditlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	portsaudit "narumq/internal/application/ports/audit"
)

// ReadAuditRecords reads a stable node-local snapshot newest first.
func (publisher *FileAuditPublisher) ReadAuditRecords(ctx context.Context, query portsaudit.AuditRecordQueryDTO) (portsaudit.AuditRecordPageDTO, error) {
	if publisher == nil || ctx == nil {
		return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(errors.New("publisher or context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
	}
	publisher.mu.Lock()
	if publisher.unavailable != nil || publisher.closed {
		err := publisher.unavailable
		if err == nil {
			err = errors.New("publisher is closed")
		}
		publisher.mu.Unlock()
		return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
	}
	snapSeq := publisher.seq
	boundary := publisher.seq
	if boundary == ^uint64(0) {
		boundary = 0
	} else {
		boundary++
	}
	var activeData []byte
	if publisher.activeSize > 0 {
		boundary = publisher.activeFirst
		if query.BeforeSeq == 0 || query.BeforeSeq > publisher.activeFirst {
			file, err := os.Open(filepath.Join(publisher.directory, activeFileName))
			if err != nil {
				publisher.mu.Unlock()
				return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
			}
			if publisher.activeSize > int64(int(^uint(0)>>1)) {
				_ = file.Close()
				publisher.mu.Unlock()
				return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(errors.New("active audit snapshot is too large"))
			}
			activeData = make([]byte, int(publisher.activeSize))
			_, readErr := io.ReadFull(file, activeData)
			closeErr := file.Close()
			if readErr != nil {
				publisher.mu.Unlock()
				return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(readErr)
			}
			if closeErr != nil {
				publisher.mu.Unlock()
				return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(closeErr)
			}
		}
	}
	directory := publisher.directory
	publisher.mu.Unlock()

	rotations, err := listAuditRotationFiles(directory)
	if err != nil {
		return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
	}
	sort.Slice(rotations, func(i, j int) bool { return rotations[i].firstSeq > rotations[j].firstSeq })
	state := auditReadState{query: query, snapSeq: snapSeq}
	if len(activeData) > 0 {
		records, scanErr := scanAuditData(activeData, activeFileName, false)
		if scanErr != nil {
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(scanErr)
		}
		if err := state.consume(records); err != nil {
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
		}
		if state.hasMore {
			return state.page(), nil
		}
	}
	for rotationIndex, rotation := range rotations {
		if err := ctx.Err(); err != nil {
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
		}
		if boundary != 0 && rotation.firstSeq >= boundary {
			continue
		}
		if query.BeforeSeq != 0 && rotation.firstSeq >= query.BeforeSeq {
			continue
		}
		data, readErr := os.ReadFile(rotation.path)
		if errors.Is(readErr, os.ErrNotExist) {
			oldestPrefix, checkErr := missingRotationIsOldestPrefix(rotations, rotationIndex)
			if checkErr != nil {
				return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(checkErr)
			}
			if oldestPrefix {
				break
			}
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(readErr)
		}
		if readErr != nil {
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(readErr)
		}
		records, scanErr := scanAuditData(data, rotation.name, false)
		if scanErr != nil {
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(scanErr)
		}
		if len(records) == 0 {
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(fmt.Errorf("%s:1: rotation file is empty", rotation.name))
		}
		if records[0].Seq != rotation.firstSeq {
			err := fmt.Errorf("%s:1: first sequence %d does not match file name sequence %d", rotation.name, records[0].Seq, rotation.firstSeq)
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
		}
		if err := state.consume(records); err != nil {
			return portsaudit.AuditRecordPageDTO{}, unavailableAuditRecords(err)
		}
		if state.hasMore {
			break
		}
	}
	return state.page(), nil
}

type auditReadState struct {
	query    portsaudit.AuditRecordQueryDTO
	snapSeq  uint64
	records  []portsaudit.AuditRecordDTO
	hasMore  bool
	nextSeq  uint64
	nextHash string
}

func (state *auditReadState) consume(records []auditLogRecord) error {
	for index := len(records) - 1; index >= 0; index-- {
		record := records[index]
		if state.nextSeq != 0 && (record.Seq+1 != state.nextSeq || state.nextHash != record.Hash) {
			return fmt.Errorf("audit chain link mismatch at sequence %d", record.Seq)
		}
		state.nextSeq, state.nextHash = record.Seq, record.PrevHash
		if record.Seq > state.snapSeq || state.query.BeforeSeq != 0 && record.Seq >= state.query.BeforeSeq {
			continue
		}
		var event auditEventWire
		if err := json.Unmarshal(record.Event, &event); err != nil {
			return fmt.Errorf("decode audit event at sequence %d: %w", record.Seq, err)
		}
		matches, err := state.matches(event, record.Seq)
		if err != nil {
			return err
		}
		if state.hasMore || !matches {
			continue
		}
		state.records = append(state.records, portsaudit.AuditRecordDTO{
			Seq: record.Seq, PrevHash: record.PrevHash, Hash: record.Hash,
			EventJSON: append([]byte(nil), record.Event...),
		})
		if len(state.records) > state.query.Limit {
			state.hasMore = true
		}
	}
	return nil
}

func (state *auditReadState) matches(event auditEventWire, sequence uint64) (bool, error) {
	occurredAt, err := time.Parse(time.RFC3339Nano, event.OccurredAt)
	if err != nil {
		return false, fmt.Errorf("invalid occurredAt at sequence %d: %w", sequence, err)
	}
	query := state.query
	if query.ActorSubject != "" && event.Actor.Subject != query.ActorSubject {
		return false, nil
	}
	if query.Action != "" && event.Action != query.Action {
		return false, nil
	}
	if query.TargetType != "" && event.Target.Type != query.TargetType {
		return false, nil
	}
	if query.TargetID != "" && event.Target.ID != query.TargetID {
		return false, nil
	}
	if query.RequestID != "" && event.RequestID != query.RequestID {
		return false, nil
	}
	if query.Result != "" && event.Result != string(query.Result) {
		return false, nil
	}
	if !query.From.IsZero() && occurredAt.Before(query.From) {
		return false, nil
	}
	if !query.To.IsZero() && !occurredAt.Before(query.To) {
		return false, nil
	}
	return true, nil
}

func (state *auditReadState) page() portsaudit.AuditRecordPageDTO {
	if state.hasMore && len(state.records) > state.query.Limit {
		state.records = state.records[:state.query.Limit]
	}
	return portsaudit.AuditRecordPageDTO{Records: state.records, HasMore: state.hasMore}
}
func missingRotationIsOldestPrefix(rotations []auditRotationFile, missingIndex int) (bool, error) {
	for _, older := range rotations[missingIndex+1:] {
		_, err := os.Lstat(older.path)
		if err == nil {
			return false, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return true, nil
}

func unavailableAuditRecords(err error) error {
	return fmt.Errorf("%w: %v", portsaudit.ErrAuditRecordsUnavailable, err)
}
