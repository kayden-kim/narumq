package tcp

import (
	"errors"
	"testing"

	"narumq/internal/domain/mqtt"
)

func TestPacketIDAllocatorSequentialAndRelease(t *testing.T) {
	alloc := newPacketIDAllocator()

	id1, err := alloc.acquire()
	if err != nil || id1 != 1 {
		t.Fatalf("acquire #1 = %v, err %v; want 1, nil", id1, err)
	}

	id2, err := alloc.acquire()
	if err != nil || id2 != 2 {
		t.Fatalf("acquire #2 = %v, err %v; want 2, nil", id2, err)
	}

	alloc.release(id1)

	// Since next points to 3, the next acquire should be 3
	id3, err := alloc.acquire()
	if err != nil || id3 != 3 {
		t.Fatalf("acquire #3 = %v, err %v; want 3, nil", id3, err)
	}
}

func TestPacketIDAllocatorExhaustion(t *testing.T) {
	alloc := newPacketIDAllocator()

	for i := 1; i <= 65535; i++ {
		id, err := alloc.acquire()
		if err != nil {
			t.Fatalf("acquire #%d failed: %v", i, err)
		}
		if id != mqtt.PacketIdentifier(i) {
			t.Fatalf("acquire #%d = %d", i, id)
		}
	}

	_, err := alloc.acquire()
	if !errors.Is(err, ErrPacketIDExhausted) {
		t.Fatalf("expected ErrPacketIDExhausted, got %v", err)
	}

	// Release one ID and verify it can be acquired
	alloc.release(42)
	id, err := alloc.acquire()
	if err != nil || id != 42 {
		t.Fatalf("acquire after release = %v, err %v; want 42, nil", id, err)
	}

	// Now full again
	_, err = alloc.acquire()
	if !errors.Is(err, ErrPacketIDExhausted) {
		t.Fatalf("expected ErrPacketIDExhausted, got %v", err)
	}
}
