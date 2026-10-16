package raft

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The vectors are produced by tests/chaos/eks-wal-torn-tail/wal_frames.py. They are the exact WAL tails
// the EKS torn-tail tool injects (ADR-0023). This test replays them against the real reader so the
// Python classifier and the Go reader cannot drift apart.
type walTailVectorFile struct {
	Format  int             `json:"format"`
	Vectors []walTailVector `json:"vectors"`
}

type walTailVector struct {
	Name       string        `json:"name"`
	WAL        []walTailPart `json:"wal"`
	Outcome    string        `json:"outcome"`
	ValidBytes int64         `json:"validBytes"`
	Frames     int           `json:"frames"`
	Reason     string        `json:"reason"`
}

type walTailPart struct {
	Hex   string `json:"hex"`
	Zeros int    `json:"zeros"`
}

func (vector walTailVector) bytes(t *testing.T) []byte {
	t.Helper()
	var data []byte
	for _, part := range vector.WAL {
		if part.Zeros > 0 {
			data = append(data, make([]byte, part.Zeros)...)
			continue
		}
		decoded, err := hex.DecodeString(part.Hex)
		if err != nil {
			t.Fatalf("%s: invalid hex: %v", vector.Name, err)
		}
		data = append(data, decoded...)
	}
	return data
}

func TestWALTailVectorsMatchGoReader(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "wal-tail-vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file walTailVectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if file.Format != 1 || len(file.Vectors) == 0 {
		t.Fatalf("vector file format = %d, vectors = %d", file.Format, len(file.Vectors))
	}
	seen := map[string]int{}
	for _, vector := range file.Vectors {
		data := vector.bytes(t)
		for _, dynamic := range []bool{false, true} {
			name := vector.Name
			if dynamic {
				name += "/dynamic"
			}
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), walName)
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
				records, err := readWALMode(path, dynamic)
				after, statErr := os.Stat(path)
				if statErr != nil {
					t.Fatal(statErr)
				}
				switch vector.Outcome {
				case "fail":
					if !errors.Is(err, ErrCorruptWAL) || !strings.Contains(err.Error(), vector.Reason) {
						t.Fatalf("error = %v, want %v containing %q", err, ErrCorruptWAL, vector.Reason)
					}
					if after.Size() != int64(len(data)) {
						t.Fatalf("a refused WAL was modified: size %d, want %d", after.Size(), len(data))
					}
				case "truncate", "ok":
					if err != nil {
						t.Fatalf("readWALMode error = %v", err)
					}
					if len(records) != vector.Frames || after.Size() != vector.ValidBytes {
						t.Fatalf("records = %d, size = %d, want %d and %d", len(records), after.Size(), vector.Frames, vector.ValidBytes)
					}
					if vector.Outcome == "ok" && after.Size() != int64(len(data)) {
						t.Fatalf("an intact WAL was cut to %d of %d bytes", after.Size(), len(data))
					}
				default:
					t.Fatalf("unknown outcome %q", vector.Outcome)
				}
			})
		}
		seen[strings.SplitN(vector.Name, "/", 2)[0]]++
	}
	for _, required := range []string{"valid", "short-header", "zero-tail", "crc-final-zero-sector", "final-crc-no-zero-evidence",
		"middle-crc", "zero-length-frame", "oversized-frame"} {
		if seen[required] == 0 {
			t.Fatalf("vector group %q is missing", required)
		}
	}
}
