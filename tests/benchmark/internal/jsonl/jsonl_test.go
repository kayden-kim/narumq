package jsonl

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriterFlushesEachRecordAndClosesIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(map[string]any{"sequence": 1}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("Write() did not flush a record")
	}
	if err := writer.Write(map[string]any{"sequence": 2}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := writer.Write(map[string]any{"sequence": 3}); err == nil {
		t.Fatal("Write() succeeded after Close()")
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	sequences := []int{}
	for scanner.Scan() {
		var value struct {
			Sequence int `json:"sequence"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, value.Sequence)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(sequences) != 2 || sequences[0] != 1 || sequences[1] != 2 {
		t.Fatalf("JSONL sequences = %#v", sequences)
	}
}
