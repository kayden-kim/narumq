package main

import (
	"encoding/binary"
	"errors"
)

func makePayload(profile Profile, streamIndex int, sequence uint64) []byte {
	payload := make([]byte, payloadSizeFor(profile, sequence))
	binary.BigEndian.PutUint64(payload[:8], sequence)
	payload[8] = byte(streamIndex)
	for offset := 9; offset < len(payload); offset++ {
		payload[offset] = deterministicByte(profile.Seed, uint64(streamIndex), sequence, offset-9)
	}
	return payload
}

func deterministicByte(seed, index, sequence uint64, offset int) byte {
	value := seed + index*0x9E3779B97F4A7C15 + sequence*0xD1B54A32D192ED03 + uint64(offset)
	value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9
	value = (value ^ (value >> 27)) * 0x94D049BB133111EB
	value ^= value >> 31
	return byte(value >> ((offset & 7) * 8))
}

func validateStreamPayload(profile Profile, payload []byte) (sequence uint64, streamIndex int, err error) {
	if len(payload) < 9 {
		return 0, 0, errors.New("delivery payload is too short")
	}
	sequence = binary.BigEndian.Uint64(payload[:8])
	streamIndex = int(payload[8])
	if sequence == 0 || streamIndex >= len(bp02Streams) {
		return 0, 0, errors.New("delivery payload sequence or stream index is invalid")
	}
	if len(payload) != payloadSizeFor(profile, sequence) {
		return 0, 0, errors.New("delivery payload size mismatch")
	}
	for offset := 9; offset < len(payload); offset++ {
		if payload[offset] != deterministicByte(profile.Seed, uint64(streamIndex), sequence, offset-9) {
			return 0, 0, errors.New("delivery payload deterministic bytes mismatch")
		}
	}
	return sequence, streamIndex, nil
}

func payloadSizeFor(profile Profile, sequence uint64) int {
	return profile.PayloadSizeCycle[(sequence-1)%uint64(len(profile.PayloadSizeCycle))]
}
