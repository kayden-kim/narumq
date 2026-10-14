package auditlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	portsaudit "narumq/internal/application/ports/audit"
)

const (
	DefaultMaxFileBytes int64 = 64 << 20
	activeFileName            = "audit.jsonl"
	genesisHash               = "0000000000000000000000000000000000000000000000000000000000000000"
)

type FileAuditPublisherConfig struct {
	Directory    string
	MaxFileBytes int64
}

type FileAuditPublisher struct {
	mu             sync.Mutex
	directory      string
	maxFileSize    int64
	file           *os.File
	activeSize     int64
	activeFirst    uint64
	seq            uint64
	prevHash       string
	unavailable    error
	openFailed     bool
	eventIDs       map[string]struct{}
	eventIDsLoaded bool
	closed         bool
}

type auditEventWire struct {
	SchemaVersion string         `json:"schemaVersion"`
	EventID       string         `json:"eventId"`
	OccurredAt    string         `json:"occurredAt"`
	RequestID     string         `json:"requestId"`
	Producer      producerWire   `json:"producer"`
	Actor         actorWire      `json:"actor"`
	Action        string         `json:"action"`
	Target        targetWire     `json:"target"`
	Result        string         `json:"result"`
	Reason        string         `json:"reason"`
	PolicyVersion string         `json:"policyVersion,omitempty"`
	SourceIP      string         `json:"sourceIp,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

type producerWire struct {
	Component  string `json:"component"`
	InstanceID string `json:"instanceId"`
	Version    string `json:"version"`
}

type actorWire struct {
	Subject      string   `json:"subject"`
	TenantID     string   `json:"tenantId,omitempty"`
	CredentialID string   `json:"credentialId,omitempty"`
	Roles        []string `json:"roles,omitempty"`
}

type targetWire struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	TenantID string `json:"tenantId,omitempty"`
}

type auditLogRecord struct {
	Seq      uint64          `json:"seq"`
	PrevHash string          `json:"prevHash"`
	Event    json.RawMessage `json:"event"`
	Hash     string          `json:"hash"`
}

type auditRotationFile struct {
	firstSeq uint64
	path     string
	name     string
}

type auditChainState struct {
	seq       uint64
	hash      string
	firstSeq  uint64
	records   uint64
	hasRecord bool
}

// OpenFileAuditPublisher opens a node-local append-only audit log.
// I/O and recovery failures leave the publisher unavailable and do not fail startup.
func OpenFileAuditPublisher(config FileAuditPublisherConfig) (*FileAuditPublisher, error) {
	if config.Directory == "" || config.MaxFileBytes <= 0 {
		return nil, errors.New("audit directory and positive max file size are required")
	}
	publisher := &FileAuditPublisher{
		directory:   config.Directory,
		maxFileSize: config.MaxFileBytes,
		prevHash:    genesisHash,
	}
	if err := publisher.open(); err != nil {
		publisher.unavailable = err
		publisher.openFailed = true
	}
	return publisher, nil
}

// Ready reports whether the publisher can append an event.
func (publisher *FileAuditPublisher) Ready() error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.unavailable != nil {
		if publisher.openFailed {
			return fmt.Errorf("open audit log: %w", publisher.unavailable)
		}
		return fmt.Errorf("audit log unavailable: %w", publisher.unavailable)
	}
	if publisher.closed {
		return errors.New("audit publisher is closed")
	}
	return nil
}

// Publish appends one event and waits for its file sync before returning.
func (publisher *FileAuditPublisher) Publish(event portsaudit.AuditEventDTO) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if err := publisher.publishLocked(event); err != nil {
		return err
	}
	if publisher.eventIDsLoaded && event.EventID != "" {
		publisher.eventIDs[event.EventID] = struct{}{}
	}
	return nil
}

// PublishOnce appends an event only if its ID is absent from the verified local chain history.
func (publisher *FileAuditPublisher) PublishOnce(event portsaudit.AuditEventDTO) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if event.EventID == "" {
		return errors.New("audit event ID is required for idempotent publication")
	}
	if publisher.unavailable != nil {
		return publisher.unavailable
	}
	if publisher.closed {
		return errors.New("audit publisher is closed")
	}
	if !publisher.eventIDsLoaded {
		eventIDs, err := publisher.loadEventIDs()
		if err != nil {
			return publisher.fail(fmt.Errorf("index audit event IDs: %w", err))
		}
		publisher.eventIDs, publisher.eventIDsLoaded = eventIDs, true
	}
	if _, exists := publisher.eventIDs[event.EventID]; exists {
		return nil
	}
	if err := publisher.publishLocked(event); err != nil {
		return err
	}
	publisher.eventIDs[event.EventID] = struct{}{}
	return nil
}

func (publisher *FileAuditPublisher) publishLocked(event portsaudit.AuditEventDTO) error {
	if publisher.unavailable != nil {
		return publisher.unavailable
	}
	if publisher.closed {
		return errors.New("audit publisher is closed")
	}
	if publisher.seq == ^uint64(0) {
		return publisher.fail(errors.New("audit sequence is exhausted"))
	}

	eventJSON, err := json.Marshal(toAuditEventWire(event))
	if err != nil {
		return publisher.fail(fmt.Errorf("encode audit event: %w", err))
	}
	sequence := publisher.seq + 1
	hash := calculateAuditHash(sequence, publisher.prevHash, eventJSON)
	line := makeAuditLine(sequence, publisher.prevHash, eventJSON, hash)

	if publisher.activeSize > 0 && publisher.activeSize+int64(len(line)) > publisher.maxFileSize {
		if err := publisher.rotate(); err != nil {
			return publisher.fail(err)
		}
	}
	if err := writeFull(publisher.file, line); err != nil {
		return publisher.fail(fmt.Errorf("write audit log: %w", err))
	}
	if err := publisher.file.Sync(); err != nil {
		return publisher.fail(fmt.Errorf("sync audit log: %w", err))
	}
	if publisher.activeSize == 0 {
		publisher.activeFirst = sequence
	}
	publisher.activeSize += int64(len(line))
	publisher.seq = sequence
	publisher.prevHash = hash
	return nil
}

// Close closes the active audit file.
func (publisher *FileAuditPublisher) Close() error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.closed {
		return nil
	}
	publisher.closed = true
	if publisher.file == nil {
		return nil
	}
	err := publisher.file.Close()
	publisher.file = nil
	return err
}

func (publisher *FileAuditPublisher) open() error {
	if err := os.MkdirAll(publisher.directory, 0o700); err != nil {
		return fmt.Errorf("create audit directory: %w", err)
	}
	rotations, err := listAuditRotationFiles(publisher.directory)
	if err != nil {
		return err
	}
	chain, err := verifyRotationFiles(rotations)
	if err != nil {
		return err
	}

	activePath := filepath.Join(publisher.directory, activeFileName)
	activeInfo, activeErr := os.Lstat(activePath)
	activeExists := activeErr == nil
	if activeExists && !activeInfo.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", activeFileName)
	}
	if activeErr != nil && !errors.Is(activeErr, os.ErrNotExist) {
		return fmt.Errorf("inspect active audit log: %w", activeErr)
	}
	var activeData []byte
	if activeExists {
		activeData, err = os.ReadFile(activePath)
		if err != nil {
			return fmt.Errorf("read active audit log: %w", err)
		}
		completeSize := int64(len(activeData))
		if len(activeData) > 0 && activeData[len(activeData)-1] != '\n' {
			lastNewline := bytes.LastIndexByte(activeData, '\n')
			completeSize = int64(lastNewline + 1)
			file, openErr := os.OpenFile(activePath, os.O_WRONLY, 0)
			if openErr != nil {
				return fmt.Errorf("open active audit log for recovery: %w", openErr)
			}
			truncateErr := file.Truncate(completeSize)
			if truncateErr == nil {
				truncateErr = file.Sync()
			}
			closeErr := file.Close()
			if truncateErr != nil {
				return fmt.Errorf("truncate torn audit tail: %w", truncateErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close recovered audit log: %w", closeErr)
			}
			activeData = activeData[:completeSize]
		}
		activeRecords, scanErr := scanAuditData(activeData, activeFileName, false)
		if scanErr != nil {
			return scanErr
		}
		chain, err = verifyRecords(activeRecords, chain, 0, activeFileName)
		if err != nil {
			return err
		}
		publisher.activeSize = completeSize
	}

	publisher.seq = chain.seq
	publisher.prevHash = chain.hash
	if publisher.prevHash == "" {
		publisher.prevHash = genesisHash
	}
	if activeExists && publisher.activeSize > 0 {
		activeRecords, scanErr := scanAuditData(activeData, activeFileName, false)
		if scanErr != nil {
			return scanErr
		}
		publisher.activeFirst = activeRecords[0].Seq
	} else if publisher.seq < ^uint64(0) {
		publisher.activeFirst = publisher.seq + 1
	}

	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if !activeExists {
		flags |= os.O_EXCL
	}
	file, err := os.OpenFile(activePath, flags, 0o600)
	if err != nil {
		return fmt.Errorf("open active audit log: %w", err)
	}
	publisher.file = file
	if !activeExists {
		if err := syncDirectory(publisher.directory); err != nil {
			return fmt.Errorf("sync audit directory after active file creation: %w", err)
		}
	}
	return nil
}

func (publisher *FileAuditPublisher) loadEventIDs() (map[string]struct{}, error) {
	rotations, err := listAuditRotationFiles(publisher.directory)
	if err != nil {
		return nil, err
	}
	chain, err := verifyRotationFiles(rotations)
	if err != nil {
		return nil, err
	}
	eventIDs := make(map[string]struct{})
	for _, rotation := range rotations {
		data, err := os.ReadFile(rotation.path)
		if err != nil {
			return nil, err
		}
		records, err := scanAuditData(data, rotation.name, false)
		if err != nil {
			return nil, err
		}
		if err := addEventIDs(eventIDs, records, rotation.name); err != nil {
			return nil, err
		}
	}
	activeData, err := os.ReadFile(filepath.Join(publisher.directory, activeFileName))
	if err != nil {
		return nil, err
	}
	activeRecords, err := scanAuditData(activeData, activeFileName, false)
	if err != nil {
		return nil, err
	}
	if _, err := verifyRecords(activeRecords, chain, 0, activeFileName); err != nil {
		return nil, err
	}
	if err := addEventIDs(eventIDs, activeRecords, activeFileName); err != nil {
		return nil, err
	}
	return eventIDs, nil
}

func addEventIDs(eventIDs map[string]struct{}, records []auditLogRecord, fileName string) error {
	for index, record := range records {
		var event struct {
			EventID string `json:"eventId"`
		}
		if err := json.Unmarshal(record.Event, &event); err != nil || event.EventID == "" {
			return fmt.Errorf("%s:%d: audit event has no valid eventId", fileName, index+1)
		}
		eventIDs[event.EventID] = struct{}{}
	}
	return nil
}

func (publisher *FileAuditPublisher) rotate() error {
	if publisher.file == nil {
		return errors.New("active audit log is not open")
	}
	if err := publisher.file.Close(); err != nil {
		publisher.file = nil
		return fmt.Errorf("close active audit log for rotation: %w", err)
	}
	publisher.file = nil
	rotationName := fmt.Sprintf("audit-%020d.jsonl", publisher.activeFirst)
	rotationPath := filepath.Join(publisher.directory, rotationName)
	if _, err := os.Lstat(rotationPath); err == nil {
		return fmt.Errorf("rotation file %s already exists", rotationName)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect rotation file %s: %w", rotationName, err)
	}
	activePath := filepath.Join(publisher.directory, activeFileName)
	if err := renameFile(activePath, rotationPath); err != nil {
		return fmt.Errorf("rotate audit log to %s: %w", rotationName, err)
	}
	if err := syncDirectory(publisher.directory); err != nil {
		return fmt.Errorf("sync audit directory after rotation: %w", err)
	}
	file, err := os.OpenFile(activePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("create active audit log after rotation: %w", err)
	}
	publisher.file = file
	if err := syncDirectory(publisher.directory); err != nil {
		return fmt.Errorf("sync audit directory after active file creation: %w", err)
	}
	publisher.activeSize = 0
	publisher.activeFirst = publisher.seq + 1
	return nil
}

func (publisher *FileAuditPublisher) fail(err error) error {
	publisher.unavailable = err
	return err
}

func toAuditEventWire(event portsaudit.AuditEventDTO) auditEventWire {
	return auditEventWire{
		SchemaVersion: event.SchemaVersion,
		EventID:       event.EventID,
		OccurredAt:    event.OccurredAt.UTC().Format(time.RFC3339Nano),
		RequestID:     event.RequestID,
		Producer: producerWire{
			Component:  event.Producer.Component,
			InstanceID: event.Producer.InstanceID,
			Version:    event.Producer.Version,
		},
		Actor: actorWire{
			Subject:      event.Actor.Subject,
			TenantID:     event.Actor.TenantID,
			CredentialID: event.Actor.CredentialID,
			Roles:        event.Actor.Roles,
		},
		Action: event.Action,
		Target: targetWire{
			Type:     event.Target.Type,
			ID:       event.Target.ID,
			TenantID: event.Target.TenantID,
		},
		Result:        string(event.Result),
		Reason:        event.Reason,
		PolicyVersion: event.PolicyVersion,
		SourceIP:      event.SourceIP,
		Details:       event.Details,
	}
}

func makeAuditLine(sequence uint64, previousHash string, eventJSON []byte, hash string) []byte {
	line := make([]byte, 0, len(eventJSON)+160)
	line = append(line, `{"seq":`...)
	line = strconv.AppendUint(line, sequence, 10)
	line = append(line, `,"prevHash":"`...)
	line = append(line, previousHash...)
	line = append(line, `","event":`...)
	line = append(line, eventJSON...)
	line = append(line, `,"hash":"`...)
	line = append(line, hash...)
	line = append(line, `"}`...)
	line = append(line, '\n')
	return line
}

func calculateAuditHash(sequence uint64, previousHash string, eventJSON []byte) string {
	hash := sha256.New()
	var sequenceBuffer [20]byte
	sequenceBytes := strconv.AppendUint(sequenceBuffer[:0], sequence, 10)
	_, _ = hash.Write(sequenceBytes)
	_, _ = io.WriteString(hash, "\n")
	_, _ = io.WriteString(hash, previousHash)
	_, _ = io.WriteString(hash, "\n")
	_, _ = hash.Write(eventJSON)
	return hex.EncodeToString(hash.Sum(nil))
}

func writeFull(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func listAuditRotationFiles(directory string) ([]auditRotationFile, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read audit directory: %w", err)
	}
	rotations := make([]auditRotationFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "audit-") || !strings.HasSuffix(name, ".jsonl") || len(name) != len("audit-")+20+len(".jsonl") {
			continue
		}
		digits := name[len("audit-") : len("audit-")+20]
		for _, digit := range digits {
			if digit < '0' || digit > '9' {
				continue
			}
		}
		firstSeq, parseErr := strconv.ParseUint(digits, 10, 64)
		if parseErr != nil {
			continue
		}
		if firstSeq == 0 {
			return nil, fmt.Errorf("invalid rotation file %s: first sequence must be positive", name)
		}
		rotations = append(rotations, auditRotationFile{
			firstSeq: firstSeq,
			path:     filepath.Join(directory, name),
			name:     name,
		})
	}
	sort.Slice(rotations, func(i, j int) bool { return rotations[i].firstSeq < rotations[j].firstSeq })
	return rotations, nil
}

func scanAuditData(data []byte, fileName string, allowTornTail bool) ([]auditLogRecord, error) {
	complete := data
	if len(data) > 0 && data[len(data)-1] != '\n' {
		if !allowTornTail {
			return nil, fmt.Errorf("%s:%d: torn tail in rotation file", fileName, bytes.Count(data, []byte{'\n'})+1)
		}
		lastNewline := bytes.LastIndexByte(data, '\n')
		complete = data[:lastNewline+1]
	}
	if len(complete) == 0 {
		return nil, nil
	}
	lines := bytes.Split(complete[:len(complete)-1], []byte{'\n'})
	records := make([]auditLogRecord, 0, len(lines))
	for index, line := range lines {
		if len(line) == 0 {
			return nil, fmt.Errorf("%s:%d: empty audit record", fileName, index+1)
		}
		var record auditLogRecord
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("%s:%d: decode audit record: %w", fileName, index+1, err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return nil, fmt.Errorf("%s:%d: trailing audit record data: %w", fileName, index+1, err)
		}
		if record.Seq == 0 || !validAuditHash(record.PrevHash) || !validAuditHash(record.Hash) {
			return nil, fmt.Errorf("%s:%d: invalid sequence or hash field", fileName, index+1)
		}
		if len(record.Event) == 0 || !json.Valid(record.Event) {
			return nil, fmt.Errorf("%s:%d: event is not valid JSON", fileName, index+1)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(record.Event, &object); err != nil || object == nil {
			return nil, fmt.Errorf("%s:%d: event must be a JSON object", fileName, index+1)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, record.Event); err != nil || !bytes.Equal(compact.Bytes(), record.Event) {
			return nil, fmt.Errorf("%s:%d: event JSON must be compact", fileName, index+1)
		}
		if record.Hash != calculateAuditHash(record.Seq, record.PrevHash, record.Event) {
			return nil, fmt.Errorf("%s:%d: audit hash mismatch", fileName, index+1)
		}
		records = append(records, record)
	}
	return records, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func validAuditHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, digit := range value {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return false
		}
	}
	return true
}
