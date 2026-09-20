package tcp

import (
	"errors"

	"narumq/internal/domain/mqtt"
)

var (
	// ErrPacketIDExhausted means that all 65535 packet identifiers are currently in use.
	ErrPacketIDExhausted = errors.New("packet identifier space exhausted")
)

type packetIDAllocator struct {
	next uint16
	used map[mqtt.PacketIdentifier]struct{}
}

func newPacketIDAllocator() *packetIDAllocator {
	return &packetIDAllocator{
		next: 1,
		used: make(map[mqtt.PacketIdentifier]struct{}),
	}
}

func (allocator *packetIDAllocator) acquire() (mqtt.PacketIdentifier, error) {
	if len(allocator.used) >= 65535 {
		return 0, ErrPacketIDExhausted
	}
	start := allocator.next
	for {
		id := mqtt.PacketIdentifier(allocator.next)
		if allocator.next == 65535 {
			allocator.next = 1
		} else {
			allocator.next++
		}
		if id == 0 {
			continue
		}
		if _, exists := allocator.used[id]; !exists {
			allocator.used[id] = struct{}{}
			return id, nil
		}
		if allocator.next == start {
			return 0, ErrPacketIDExhausted
		}
	}
}

func (allocator *packetIDAllocator) release(id mqtt.PacketIdentifier) {
	delete(allocator.used, id)
}
