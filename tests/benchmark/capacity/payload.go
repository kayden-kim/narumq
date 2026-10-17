package main

import (
	"encoding/binary"
	"errors"
)

const payloadHeaderBytes = 24

// payloadHeader is the fixed prefix of every message: sequence, run token, cell and stream.
type payloadHeader struct {
	seq    uint64
	token  uint32
	cell   int
	stream int
}

func payloadSize(p Profile, seq uint64) int {
	return p.PayloadSizeCycle[(seq-1)%uint64(len(p.PayloadSizeCycle))]
}

func splitmix64(state *uint64) uint64 {
	*state += 0x9E3779B97F4A7C15
	value := *state
	value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9
	value = (value ^ (value >> 27)) * 0x94D049BB133111EB
	return value ^ (value >> 31)
}

func fillSeed(seed uint64, cell, stream int, seq uint64) uint64 {
	return seed ^ uint64(cell)*0xD1B54A32D192ED03 ^ uint64(stream)*0x9E3779B97F4A7C15 ^ seq*0xA24BAED4963EE407
}

// makePayload builds a deterministic payload whose size follows the profile cycle.
func makePayload(p Profile, header payloadHeader) []byte {
	payload := make([]byte, payloadSize(p, header.seq))
	binary.BigEndian.PutUint64(payload[0:8], header.seq)
	binary.BigEndian.PutUint32(payload[8:12], header.token)
	binary.BigEndian.PutUint32(payload[12:16], uint32(header.cell))
	payload[16] = byte(header.stream)
	state := fillSeed(p.Seed, header.cell, header.stream, header.seq)
	for offset := payloadHeaderBytes; offset < len(payload); offset += 8 {
		word := splitmix64(&state)
		if len(payload)-offset >= 8 {
			binary.LittleEndian.PutUint64(payload[offset:], word)
			continue
		}
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], word)
		copy(payload[offset:], tail[:])
	}
	return payload
}

// parsePayloadHeader reads the header without validating the body.
func parsePayloadHeader(payload []byte) (payloadHeader, error) {
	if len(payload) < payloadHeaderBytes+8 {
		return payloadHeader{}, errors.New("delivery payload is too short")
	}
	header := payloadHeader{
		seq:    binary.BigEndian.Uint64(payload[0:8]),
		token:  binary.BigEndian.Uint32(payload[8:12]),
		cell:   int(binary.BigEndian.Uint32(payload[12:16])),
		stream: int(payload[16]),
	}
	if header.seq == 0 || header.stream >= cellStreams {
		return payloadHeader{}, errors.New("delivery payload sequence or stream is invalid")
	}
	return header, nil
}

// verifyPayload checks size and every deterministic body byte against the header.
func verifyPayload(p Profile, header payloadHeader, payload []byte) error {
	if len(payload) != payloadSize(p, header.seq) {
		return errors.New("delivery payload size mismatch")
	}
	state := fillSeed(p.Seed, header.cell, header.stream, header.seq)
	for offset := payloadHeaderBytes; offset < len(payload); offset += 8 {
		word := splitmix64(&state)
		if len(payload)-offset >= 8 {
			if binary.LittleEndian.Uint64(payload[offset:]) != word {
				return errors.New("delivery payload bytes mismatch")
			}
			continue
		}
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], word)
		for index := offset; index < len(payload); index++ {
			if payload[index] != tail[index-offset] {
				return errors.New("delivery payload bytes mismatch")
			}
		}
	}
	return nil
}
