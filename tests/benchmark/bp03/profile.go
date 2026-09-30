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
	"math/bits"
	"os"
	"strings"
	"time"
)

const purposeSmoke = "smoke"

// Profile defines the fixed BP-03 durable-delivery smoke workload.
type Profile struct {
	ProfileID                string   `json:"profileId"`
	Purpose                  string   `json:"purpose"`
	Seed                     uint64   `json:"seed"`
	PairsPerQoS              int      `json:"pairsPerQoS"`
	PayloadBytes             int      `json:"payloadBytes"`
	SessionExpirySeconds     uint32   `json:"sessionExpirySeconds"`
	MaxInflightPerPublisher  int      `json:"maxInflightPerPublisher"`
	AckTimeoutMillis         int64    `json:"ackTimeoutMillis"`
	RatesPerStreamPerSecond  []uint64 `json:"ratesPerStreamPerSecond"`
	WarmupMillis             int64    `json:"warmupMillis"`
	MeasureMillis            int64    `json:"measureMillis"`
	DrainMillis              int64    `json:"drainMillis"`
	OfflineSettleMillis      int64    `json:"offlineSettleMillis"`
	OfflineMessagesPerStream int      `json:"offlineMessagesPerStream"`
	ReconnectTimeoutMillis   int64    `json:"reconnectTimeoutMillis"`
	MaxMessages              uint64   `json:"maxMessages"`
	BrokerCPUs               float64  `json:"brokerCPUs"`
	BrokerMemoryBytes        int64    `json:"brokerMemoryBytes"`
	GeneratorCPUs            float64  `json:"generatorCPUs"`
	GeneratorMemoryBytes     int64    `json:"generatorMemoryBytes"`
}

// Validate checks the BP-03 smoke bounds before Docker work or message allocation.
func (p Profile) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return errors.New("profileId is required")
	}
	if p.Purpose != purposeSmoke {
		return fmt.Errorf("purpose %q is unsupported; only %q is allowed", p.Purpose, purposeSmoke)
	}
	if p.PairsPerQoS < 1 || p.PairsPerQoS > 8 {
		return errors.New("pairsPerQoS must be between 1 and 8")
	}
	if p.PayloadBytes < 16 || p.PayloadBytes > 65536 {
		return errors.New("payloadBytes must be between 16 and 65536")
	}
	if p.SessionExpirySeconds < 1 || p.SessionExpirySeconds > 86400 {
		return errors.New("sessionExpirySeconds must be between 1 and 86400")
	}
	if p.MaxInflightPerPublisher < 1 || p.MaxInflightPerPublisher > 32 {
		return errors.New("maxInflightPerPublisher must be between 1 and 32")
	}
	if p.AckTimeoutMillis < 1000 || p.AckTimeoutMillis > 60000 {
		return errors.New("ackTimeoutMillis must be between 1000 and 60000")
	}
	if len(p.RatesPerStreamPerSecond) < 1 || len(p.RatesPerStreamPerSecond) > 8 {
		return errors.New("ratesPerStreamPerSecond must contain between 1 and 8 rates")
	}
	for index, rate := range p.RatesPerStreamPerSecond {
		if rate < 1 || rate > 1000 {
			return fmt.Errorf("ratesPerStreamPerSecond[%d] must be between 1 and 1000", index)
		}
		if index > 0 && rate <= p.RatesPerStreamPerSecond[index-1] {
			return errors.New("ratesPerStreamPerSecond must be strictly increasing")
		}
	}
	if p.WarmupMillis <= 0 {
		return errors.New("warmupMillis must be greater than zero")
	}
	if p.MeasureMillis < 3000 {
		return errors.New("measureMillis must be at least 3000")
	}
	if p.DrainMillis <= 0 {
		return errors.New("drainMillis must be greater than zero")
	}
	if p.OfflineSettleMillis < 0 {
		return errors.New("offlineSettleMillis must not be negative")
	}
	if p.OfflineMessagesPerStream < 1 || p.OfflineMessagesPerStream > 48 {
		return errors.New("offlineMessagesPerStream must be between 1 and 48")
	}
	if p.ReconnectTimeoutMillis < 1000 {
		return errors.New("reconnectTimeoutMillis must be at least 1000")
	}
	for _, durationMillis := range []int64{p.AckTimeoutMillis, p.WarmupMillis, p.MeasureMillis, p.DrainMillis, p.OfflineSettleMillis, p.ReconnectTimeoutMillis} {
		if durationMillis > math.MaxInt64/int64(time.Millisecond) {
			return errors.New("profile duration exceeds supported range")
		}
	}
	if !positiveFinite(p.BrokerCPUs) || p.BrokerMemoryBytes <= 0 || !positiveFinite(p.GeneratorCPUs) || p.GeneratorMemoryBytes <= 0 {
		return errors.New("broker and generator resource limits must be positive finite values")
	}
	messageCount, err := p.expectedMessageCount()
	if err != nil {
		return err
	}
	if p.MaxMessages < messageCount {
		return fmt.Errorf("maxMessages %d is below the computed workload total %d", p.MaxMessages, messageCount)
	}
	return nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func (p Profile) expectedMessageCount() (uint64, error) {
	streams := uint64(p.PairsPerQoS) * 2
	durationMillis := uint64(p.WarmupMillis + p.MeasureMillis)
	var total uint64
	for _, rate := range p.RatesPerStreamPerSecond {
		productHi, productLo := bits.Mul64(rate, streams)
		if productHi != 0 {
			return 0, errors.New("computed message count overflows uint64")
		}
		productHi, productLo = bits.Mul64(productLo, durationMillis)
		if productHi != 0 {
			return 0, errors.New("computed message count overflows uint64")
		}
		count := productLo / 1000
		if math.MaxUint64-total < count {
			return 0, errors.New("computed message count overflows uint64")
		}
		total += count
	}
	offline := uint64(p.OfflineMessagesPerStream) * streams
	if math.MaxUint64-total < offline {
		return 0, errors.New("computed message count overflows uint64")
	}
	return total + offline, nil
}

type profileJSON struct {
	ProfileID                *string   `json:"profileId"`
	Purpose                  *string   `json:"purpose"`
	Seed                     *uint64   `json:"seed"`
	PairsPerQoS              *int64    `json:"pairsPerQoS"`
	PayloadBytes             *int64    `json:"payloadBytes"`
	SessionExpirySeconds     *uint64   `json:"sessionExpirySeconds"`
	MaxInflightPerPublisher  *int64    `json:"maxInflightPerPublisher"`
	AckTimeoutMillis         *int64    `json:"ackTimeoutMillis"`
	RatesPerStreamPerSecond  *[]uint64 `json:"ratesPerStreamPerSecond"`
	WarmupMillis             *int64    `json:"warmupMillis"`
	MeasureMillis            *int64    `json:"measureMillis"`
	DrainMillis              *int64    `json:"drainMillis"`
	OfflineSettleMillis      *int64    `json:"offlineSettleMillis"`
	OfflineMessagesPerStream *int64    `json:"offlineMessagesPerStream"`
	ReconnectTimeoutMillis   *int64    `json:"reconnectTimeoutMillis"`
	MaxMessages              *uint64   `json:"maxMessages"`
	BrokerCPUs               *float64  `json:"brokerCPUs"`
	BrokerMemoryBytes        *int64    `json:"brokerMemoryBytes"`
	GeneratorCPUs            *float64  `json:"generatorCPUs"`
	GeneratorMemoryBytes     *int64    `json:"generatorMemoryBytes"`
}

// DecodeProfile strictly decodes and validates one BP-03 profile.
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
	for _, value := range []int64{*raw.PairsPerQoS, *raw.PayloadBytes, *raw.MaxInflightPerPublisher, *raw.OfflineMessagesPerStream} {
		if value < 0 || uint64(value) > uint64(maxInt()) {
			return Profile{}, errors.New("profile integer exceeds supported range")
		}
	}
	if *raw.SessionExpirySeconds > math.MaxUint32 {
		return Profile{}, errors.New("sessionExpirySeconds exceeds MQTT uint32 range")
	}
	profile := Profile{
		ProfileID:                *raw.ProfileID,
		Purpose:                  *raw.Purpose,
		Seed:                     *raw.Seed,
		PairsPerQoS:              int(*raw.PairsPerQoS),
		PayloadBytes:             int(*raw.PayloadBytes),
		SessionExpirySeconds:     uint32(*raw.SessionExpirySeconds),
		MaxInflightPerPublisher:  int(*raw.MaxInflightPerPublisher),
		AckTimeoutMillis:         *raw.AckTimeoutMillis,
		RatesPerStreamPerSecond:  append([]uint64(nil), (*raw.RatesPerStreamPerSecond)...),
		WarmupMillis:             *raw.WarmupMillis,
		MeasureMillis:            *raw.MeasureMillis,
		DrainMillis:              *raw.DrainMillis,
		OfflineSettleMillis:      *raw.OfflineSettleMillis,
		OfflineMessagesPerStream: int(*raw.OfflineMessagesPerStream),
		ReconnectTimeoutMillis:   *raw.ReconnectTimeoutMillis,
		MaxMessages:              *raw.MaxMessages,
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
	case raw.Seed == nil:
		return "seed"
	case raw.PairsPerQoS == nil:
		return "pairsPerQoS"
	case raw.PayloadBytes == nil:
		return "payloadBytes"
	case raw.SessionExpirySeconds == nil:
		return "sessionExpirySeconds"
	case raw.MaxInflightPerPublisher == nil:
		return "maxInflightPerPublisher"
	case raw.AckTimeoutMillis == nil:
		return "ackTimeoutMillis"
	case raw.RatesPerStreamPerSecond == nil:
		return "ratesPerStreamPerSecond"
	case raw.WarmupMillis == nil:
		return "warmupMillis"
	case raw.MeasureMillis == nil:
		return "measureMillis"
	case raw.DrainMillis == nil:
		return "drainMillis"
	case raw.OfflineSettleMillis == nil:
		return "offlineSettleMillis"
	case raw.OfflineMessagesPerStream == nil:
		return "offlineMessagesPerStream"
	case raw.ReconnectTimeoutMillis == nil:
		return "reconnectTimeoutMillis"
	case raw.MaxMessages == nil:
		return "maxMessages"
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

// LoadProfile returns the validated profile, exact source bytes and their SHA-256.
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
