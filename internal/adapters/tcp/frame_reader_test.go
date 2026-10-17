package tcp

import (
	"bytes"
	"errors"
	"testing"

	"narumq/internal/domain/mqtt"
)

// Traceability: PRD MQTT-12/TEST-04/05.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-030
func TestFrameReaderStartsSmallGrowsByDoublingAndReleases(t *testing.T) {
	for _, maximum := range []int{64, 10000} {
		reader := newFrameReader(maximum)
		initial := min(4096, maximum+5)
		if len(reader.buffer) != initial {
			t.Fatalf("initial buffer = %d, want %d", len(reader.buffer), initial)
		}
		for len(reader.buffer) < reader.limit {
			old := len(reader.buffer)
			reader.buffered = old - 1
			if err := reader.grow(); err != nil || len(reader.buffer) != old {
				t.Fatalf("premature grow: %d, %v", len(reader.buffer), err)
			}
			reader.buffered = old
			for index := range reader.buffer {
				reader.buffer[index] = byte(index)
			}
			expected := append([]byte(nil), reader.pending()...)
			if err := reader.grow(); err != nil || len(reader.buffer) != min(2*old, reader.limit) || !bytes.Equal(reader.pending(), expected) {
				t.Fatalf("grow: length %d, error %v", len(reader.buffer), err)
			}
		}
		reader.buffered = reader.limit
		if err := reader.grow(); !errors.Is(err, mqtt.ErrPacketTooLarge) {
			t.Fatalf("limit error = %v", err)
		}
		reader.consume(reader.buffered - 2)
		if reader.buffered != 2 {
			t.Fatalf("partial consume leaves %d", reader.buffered)
		}
		reader.consume(2)
		if reader.buffered != 0 || len(reader.buffer) != initial {
			t.Fatalf("empty buffer: %d bytes, capacity %d", reader.buffered, len(reader.buffer))
		}
	}
}
