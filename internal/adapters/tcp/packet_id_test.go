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

func TestPacketIDAllocatorReserveRejectsZeroAndDuplicates(t *testing.T) {
	alloc := newPacketIDAllocator()
	if alloc.reserve(0) {
		t.Fatal("reserve(0) unexpectedly succeeded")
	}
	if !alloc.reserve(42) {
		t.Fatal("first reserve(42) failed")
	}
	if alloc.reserve(42) {
		t.Fatal("duplicate reserve(42) unexpectedly succeeded")
	}
	alloc.release(42)
	if !alloc.reserve(42) {
		t.Fatal("reserve(42) after release failed")
	}
}

func TestPacketIDAllocatorReserveAllIDs(t *testing.T) {
	alloc := newPacketIDAllocator()
	for id := mqtt.PacketIdentifier(1); id != 0; id++ {
		if !alloc.reserve(id) {
			t.Fatalf("reserve(%d) failed", id)
		}
	}
	if alloc.reserve(1) {
		t.Fatal("reserve(1) unexpectedly succeeded after exhaustion")
	}
}

func TestPacketIDAllocatorAcquireWrapsAndSkipsUsed(t *testing.T) {
	alloc := newPacketIDAllocator()
	alloc.next = 65535
	if !alloc.reserve(1) {
		t.Fatal("reserve(1) failed")
	}
	if !alloc.reserve(65534) {
		t.Fatal("reserve(65534) failed")
	}
	id, err := alloc.acquire()
	if err != nil || id != 65535 {
		t.Fatalf("acquire at wrap boundary = %d, %v; want 65535, nil", id, err)
	}
	id, err = alloc.acquire()
	if err != nil || id != 2 {
		t.Fatalf("acquire after wrap and skip = %d, %v; want 2, nil", id, err)
	}
}
