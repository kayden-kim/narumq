// Package jsonl provides the shared line-buffered artifact writer used by benchmark runners.
package jsonl

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Writer serializes JSON values as flushed newline-delimited records.
type Writer struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

// Open creates or truncates a JSONL artifact at path.
func Open(path string) (*Writer, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	return &Writer{file: file, writer: bufio.NewWriterSize(file, 64*1024)}, nil
}

// Write encodes one JSON value, appends its newline, and flushes it to disk.
func (writer *Writer) Write(value any) error {
	if writer == nil {
		return errors.New("JSONL writer is nil")
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.file == nil || writer.writer == nil {
		return errors.New("JSONL writer is closed")
	}
	if err := json.NewEncoder(writer.writer).Encode(value); err != nil {
		return err
	}
	return writer.writer.Flush()
}

// Close flushes and closes the artifact; repeated calls are no-ops.
func (writer *Writer) Close() error {
	if writer == nil {
		return nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.file == nil {
		return nil
	}
	flushErr := writer.writer.Flush()
	closeErr := writer.file.Close()
	writer.file = nil
	writer.writer = nil
	if flushErr != nil {
		if closeErr != nil {
			return errors.Join(fmt.Errorf("flush JSONL writer: %w", flushErr), fmt.Errorf("close JSONL writer: %w", closeErr))
		}
		return fmt.Errorf("flush JSONL writer: %w", flushErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close JSONL writer: %w", closeErr)
	}
	return nil
}
