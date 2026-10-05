package tcp

import (
	"net"

	"narumq/internal/domain/mqtt"
)

const initialFrameBufferSize = 4 << 10

// frameReader grows only after received bytes fill its buffer and returns to
// its initial size when no bytes are pending.
type frameReader struct {
	buffer   []byte
	buffered int
	initial  int
	limit    int
}

func newFrameReader(maxPacketSize int) *frameReader {
	limit := maxPacketSize + 5
	initial := min(initialFrameBufferSize, limit)
	return &frameReader{buffer: make([]byte, initial), initial: initial, limit: limit}
}

func (reader *frameReader) pending() []byte { return reader.buffer[:reader.buffered] }

func (reader *frameReader) readFrom(socket net.Conn) (int, error) {
	count, err := socket.Read(reader.buffer[reader.buffered:])
	reader.buffered += count
	return count, err
}

func (reader *frameReader) grow() error {
	if reader.buffered != len(reader.buffer) {
		return nil
	}
	if len(reader.buffer) == reader.limit {
		return mqtt.ErrPacketTooLarge
	}
	buffer := make([]byte, min(2*len(reader.buffer), reader.limit))
	copy(buffer, reader.pending())
	reader.buffer = buffer
	return nil
}

func (reader *frameReader) consume(count int) {
	copy(reader.buffer, reader.buffer[count:reader.buffered])
	reader.buffered -= count
	if reader.buffered == 0 && len(reader.buffer) > reader.initial {
		reader.buffer = make([]byte, reader.initial)
	}
}
