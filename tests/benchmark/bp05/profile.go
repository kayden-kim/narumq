package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

const purposeSmoke = "smoke"

// Profile is the deliberately small BP-05 connection-scale smoke input.
type Profile struct {
	ProfileID                string  `json:"profileId"`
	Purpose                  string  `json:"purpose"`
	ConnectionCount          int     `json:"connectionCount"`
	RampConnectionsPerSecond uint64  `json:"rampConnectionsPerSecond"`
	BaselineMillis           int64   `json:"baselineMillis"`
	IdleMillis               int64   `json:"idleMillis"`
	ReconnectCount           int     `json:"reconnectCount"`
	ReconnectRatePerSecond   uint64  `json:"reconnectRatePerSecond"`
	KeepAliveSeconds         int64   `json:"keepAliveSeconds"`
	PingIntervalMillis       int64   `json:"pingIntervalMillis"`
	BrokerCPUs               float64 `json:"brokerCPUs"`
	BrokerMemoryBytes        int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs            float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes     int64   `json:"generatorMemoryBytes"`
}

func (p Profile) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return errors.New("profileId is required")
	}
	if strings.TrimSpace(p.Purpose) == "" {
		return errors.New("purpose is required")
	}
	if p.Purpose != purposeSmoke {
		return fmt.Errorf("purpose %q is unsupported; only %q is allowed", p.Purpose, purposeSmoke)
	}
	if p.ConnectionCount <= 0 {
		return errors.New("connectionCount must be greater than zero")
	}
	if p.RampConnectionsPerSecond == 0 || p.ReconnectRatePerSecond == 0 {
		return errors.New("rampConnectionsPerSecond and reconnectRatePerSecond must be greater than zero")
	}
	if p.BaselineMillis <= 0 || p.IdleMillis <= 0 {
		return errors.New("baselineMillis and idleMillis must be greater than zero")
	}
	if p.ReconnectCount < 0 || p.ReconnectCount > p.ConnectionCount {
		return fmt.Errorf("reconnectCount must be between zero and connectionCount (%d)", p.ConnectionCount)
	}
	if p.KeepAliveSeconds <= 0 {
		return errors.New("keepAliveSeconds must be greater than zero")
	}
	if p.PingIntervalMillis <= 0 {
		return errors.New("pingIntervalMillis must be greater than zero")
	}
	if p.KeepAliveSeconds > (1<<16)-1 {
		return errors.New("keepAliveSeconds exceeds MQTT uint16 range")
	}
	if p.KeepAliveSeconds > math.MaxInt64/1000 {
		return errors.New("keepAliveSeconds overflows milliseconds")
	}
	keepAliveMillis := p.KeepAliveSeconds * 1000
	if p.PingIntervalMillis >= keepAliveMillis {
		return fmt.Errorf("pingIntervalMillis must be less than keepAliveSeconds*1000 (%d)", keepAliveMillis)
	}
	if p.PingIntervalMillis > math.MaxInt64/2 || p.IdleMillis < 2*p.PingIntervalMillis {
		return errors.New("idleMillis must be at least twice pingIntervalMillis")
	}
	if p.BaselineMillis < 2000 {
		return errors.New("baselineMillis must be at least 2000")
	}
	if p.RampConnectionsPerSecond > uint64(math.MaxInt64) || p.ReconnectRatePerSecond > uint64(math.MaxInt64) {
		return errors.New("connection rates exceed supported duration range")
	}
	if p.KeepAliveSeconds > math.MaxInt64/int64(time.Second) || p.BaselineMillis > math.MaxInt64/int64(time.Millisecond) || p.IdleMillis > math.MaxInt64/int64(time.Millisecond) || p.PingIntervalMillis > math.MaxInt64/int64(time.Millisecond) {
		return errors.New("profile duration exceeds supported range")
	}
	if !positiveFinite(p.BrokerCPUs) || p.BrokerMemoryBytes <= 0 || !positiveFinite(p.GeneratorCPUs) || p.GeneratorMemoryBytes <= 0 {
		return errors.New("broker and generator resource limits must be positive finite values")
	}
	return nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

type profileJSON struct {
	ProfileID                *string  `json:"profileId"`
	Purpose                  *string  `json:"purpose"`
	ConnectionCount          *int64   `json:"connectionCount"`
	RampConnectionsPerSecond *uint64  `json:"rampConnectionsPerSecond"`
	BaselineMillis           *int64   `json:"baselineMillis"`
	IdleMillis               *int64   `json:"idleMillis"`
	ReconnectCount           *int64   `json:"reconnectCount"`
	ReconnectRatePerSecond   *uint64  `json:"reconnectRatePerSecond"`
	KeepAliveSeconds         *int64   `json:"keepAliveSeconds"`
	PingIntervalMillis       *int64   `json:"pingIntervalMillis"`
	BrokerCPUs               *float64 `json:"brokerCPUs"`
	BrokerMemoryBytes        *int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs            *float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes     *int64   `json:"generatorMemoryBytes"`
}

// DecodeProfile decodes one strict BP-05 profile before any Docker work.
func DecodeProfile(data []byte) (Profile, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var raw profileJSON
	if err := decoder.Decode(&raw); err != nil {
		return Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Profile{}, errors.New("decode profile: multiple JSON values")
		}
		return Profile{}, fmt.Errorf("decode profile trailer: %w", err)
	}
	if missing := missingProfileField(raw); missing != "" {
		return Profile{}, fmt.Errorf("profile field %q is required", missing)
	}
	if *raw.ConnectionCount < 0 || *raw.ReconnectCount < 0 {
		return Profile{}, errors.New("connectionCount and reconnectCount must not be negative")
	}
	if uint64(*raw.ConnectionCount) > uint64(maxInt()) || uint64(*raw.ReconnectCount) > uint64(maxInt()) {
		return Profile{}, errors.New("connectionCount and reconnectCount exceed platform int range")
	}
	profile := Profile{
		ProfileID:                *raw.ProfileID,
		Purpose:                  *raw.Purpose,
		ConnectionCount:          int(*raw.ConnectionCount),
		RampConnectionsPerSecond: *raw.RampConnectionsPerSecond,
		BaselineMillis:           *raw.BaselineMillis,
		IdleMillis:               *raw.IdleMillis,
		ReconnectCount:           int(*raw.ReconnectCount),
		ReconnectRatePerSecond:   *raw.ReconnectRatePerSecond,
		KeepAliveSeconds:         *raw.KeepAliveSeconds,
		PingIntervalMillis:       *raw.PingIntervalMillis,
		BrokerCPUs:               *raw.BrokerCPUs,
		BrokerMemoryBytes:        *raw.BrokerMemoryBytes,
		GeneratorCPUs:            *raw.GeneratorCPUs,
		GeneratorMemoryBytes:     *raw.GeneratorMemoryBytes,
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, fmt.Errorf("validate profile: %w", err)
	}
	return profile, nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func missingProfileField(raw profileJSON) string {
	switch {
	case raw.ProfileID == nil:
		return "profileId"
	case raw.Purpose == nil:
		return "purpose"
	case raw.ConnectionCount == nil:
		return "connectionCount"
	case raw.RampConnectionsPerSecond == nil:
		return "rampConnectionsPerSecond"
	case raw.BaselineMillis == nil:
		return "baselineMillis"
	case raw.IdleMillis == nil:
		return "idleMillis"
	case raw.ReconnectCount == nil:
		return "reconnectCount"
	case raw.ReconnectRatePerSecond == nil:
		return "reconnectRatePerSecond"
	case raw.KeepAliveSeconds == nil:
		return "keepAliveSeconds"
	case raw.PingIntervalMillis == nil:
		return "pingIntervalMillis"
	case raw.BrokerCPUs == nil:
		return "brokerCPUs"
	case raw.BrokerMemoryBytes == nil:
		return "brokerMemoryBytes"
	case raw.GeneratorCPUs == nil:
		return "generatorCPUs"
	case raw.GeneratorMemoryBytes == nil:
		return "generatorMemoryBytes"
	default:
		return ""
	}
}

// LoadProfile returns the validated profile, original bytes and SHA-256.
func LoadProfile(path string) (Profile, []byte, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, nil, "", fmt.Errorf("read profile %q: %w", path, err)
	}
	profile, err := DecodeProfile(data)
	if err != nil {
		return Profile{}, nil, "", err
	}
	digest := sha256.Sum256(data)
	return profile, data, hex.EncodeToString(digest[:]), nil
}
