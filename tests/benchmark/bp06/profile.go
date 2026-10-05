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
)

const purposeSmoke = "smoke"

// Profile defines the fixed BP-06 failure-recovery smoke workload.
type Profile struct {
	ProfileID                     string  `json:"profileId"`
	Purpose                       string  `json:"purpose"`
	Seed                          uint64  `json:"seed"`
	PayloadBytes                  int     `json:"payloadBytes"`
	PublishRatePerStreamPerSecond uint64  `json:"publishRatePerStreamPerSecond"`
	MaxInflightPerPublisher       int     `json:"maxInflightPerPublisher"`
	AckTimeoutMillis              int64   `json:"ackTimeoutMillis"`
	SessionExpirySeconds          uint32  `json:"sessionExpirySeconds"`
	KeepAliveSeconds              uint16  `json:"keepAliveSeconds"`
	IdleClients                   int     `json:"idleClients"`
	WarmupMillis                  int64   `json:"warmupMillis"`
	SteadyMillis                  int64   `json:"steadyMillis"`
	KillOrder                     []int   `json:"killOrder"`
	Cycles                        int     `json:"cycles"`
	MarkerAckTimeoutMillis        int64   `json:"markerAckTimeoutMillis"`
	KillTimeoutMillis             int64   `json:"killTimeoutMillis"`
	DownMillis                    int64   `json:"downMillis"`
	ReadyTimeoutMillis            int64   `json:"readyTimeoutMillis"`
	ResyncTimeoutMillis           int64   `json:"resyncTimeoutMillis"`
	InterRoundSettleMillis        int64   `json:"interRoundSettleMillis"`
	ReconnectTimeoutMillis        int64   `json:"reconnectTimeoutMillis"`
	RecoveryTimeoutMillis         int64   `json:"recoveryTimeoutMillis"`
	HealthPollMillis              int64   `json:"healthPollMillis"`
	SettleMillis                  int64   `json:"settleMillis"`
	MaxMessages                   uint64  `json:"maxMessages"`
	BrokerCPUs                    float64 `json:"brokerCPUs"`
	BrokerMemoryBytes             int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs                 float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes          int64   `json:"generatorMemoryBytes"`
}

// Validate checks workload bounds before Docker work or message allocation.
func (p Profile) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" {
		return errors.New("profileId is required")
	}
	if p.Purpose != purposeSmoke {
		return errors.New("purpose must be smoke")
	}
	if p.PayloadBytes < 16 || p.PayloadBytes > 65536 {
		return errors.New("payloadBytes must be between 16 and 65536")
	}
	if p.PublishRatePerStreamPerSecond < 1 || p.PublishRatePerStreamPerSecond > 100 {
		return errors.New("publishRatePerStreamPerSecond must be between 1 and 100")
	}
	if p.MaxInflightPerPublisher < 1 || p.MaxInflightPerPublisher > 32 {
		return errors.New("maxInflightPerPublisher must be between 1 and 32")
	}
	if p.SessionExpirySeconds < 60 || p.SessionExpirySeconds > 86400 {
		return errors.New("sessionExpirySeconds must be between 60 and 86400")
	}
	if p.KeepAliveSeconds < 5 || p.KeepAliveSeconds > 300 {
		return errors.New("keepAliveSeconds must be between 5 and 300")
	}
	if p.IdleClients < 0 || p.IdleClients > 11 {
		return errors.New("idleClients must be between 0 and 11")
	}
	if p.Cycles < 1 || p.Cycles > 10 {
		return errors.New("cycles must be between 1 and 10")
	}
	for _, bound := range []struct {
		name                    string
		value, minimum, maximum int64
	}{
		{"ackTimeoutMillis", p.AckTimeoutMillis, 1000, 60000},
		{"warmupMillis", p.WarmupMillis, 1000, 600000},
		{"steadyMillis", p.SteadyMillis, 2000, 600000},
		{"markerAckTimeoutMillis", p.MarkerAckTimeoutMillis, 1000, 60000},
		{"killTimeoutMillis", p.KillTimeoutMillis, 1000, 60000},
		{"downMillis", p.DownMillis, 1000, 600000},
		{"readyTimeoutMillis", p.ReadyTimeoutMillis, 1000, 600000},
		{"resyncTimeoutMillis", p.ResyncTimeoutMillis, 1000, 600000},
		{"interRoundSettleMillis", p.InterRoundSettleMillis, 2000, 600000},
		{"reconnectTimeoutMillis", p.ReconnectTimeoutMillis, 1000, 600000},
		{"recoveryTimeoutMillis", p.RecoveryTimeoutMillis, 1000, 600000},
		{"healthPollMillis", p.HealthPollMillis, 50, 5000},
		{"settleMillis", p.SettleMillis, 1000, 600000},
	} {
		if bound.value < bound.minimum || bound.value > bound.maximum {
			return fmt.Errorf("%s must be between %d and %d", bound.name, bound.minimum, bound.maximum)
		}
	}
	if int64(p.SessionExpirySeconds)*1000 <= p.ReconnectTimeoutMillis+p.DownMillis {
		return errors.New("session expiry must exceed reconnect timeout plus down duration")
	}
	if p.ResyncTimeoutMillis < p.ReadyTimeoutMillis {
		return errors.New("resync timeout must cover ready timeout")
	}
	if p.RecoveryTimeoutMillis < p.ReconnectTimeoutMillis {
		return errors.New("recovery timeout must cover reconnect timeout")
	}
	if len(p.KillOrder) != 3 {
		return errors.New("killOrder must be a permutation of 1,2,3")
	}
	var seen [4]bool
	for _, n := range p.KillOrder {
		if n < 1 || n > 3 || seen[n] {
			return errors.New("killOrder must be a permutation of 1,2,3")
		}
		seen[n] = true
	}
	if p.MaxMessages < 1 || p.MaxMessages > 1000000 {
		return errors.New("maxMessages must be between 1 and 1000000")
	}
	if p.MaxMessages < 4*p.PublishRatePerStreamPerSecond*uint64(p.WarmupMillis+p.SteadyMillis)/1000 {
		return errors.New("maxMessages is below the warmup and steady capacity")
	}
	if !positiveFinite(p.BrokerCPUs) || !positiveFinite(p.GeneratorCPUs) || p.BrokerMemoryBytes <= 0 || p.GeneratorMemoryBytes <= 0 {
		return errors.New("resource limits must be positive finite values")
	}
	return nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
func (p Profile) rounds() int  { return 3 * p.Cycles }
func (p Profile) clients() int { return 8 + p.IdleClients }

type profileJSON struct {
	ProfileID                     *string  `json:"profileId"`
	Purpose                       *string  `json:"purpose"`
	Seed                          *uint64  `json:"seed"`
	PayloadBytes                  *int     `json:"payloadBytes"`
	PublishRatePerStreamPerSecond *uint64  `json:"publishRatePerStreamPerSecond"`
	MaxInflightPerPublisher       *int     `json:"maxInflightPerPublisher"`
	AckTimeoutMillis              *int64   `json:"ackTimeoutMillis"`
	SessionExpirySeconds          *uint32  `json:"sessionExpirySeconds"`
	KeepAliveSeconds              *uint16  `json:"keepAliveSeconds"`
	IdleClients                   *int     `json:"idleClients"`
	WarmupMillis                  *int64   `json:"warmupMillis"`
	SteadyMillis                  *int64   `json:"steadyMillis"`
	KillOrder                     *[]int   `json:"killOrder"`
	Cycles                        *int     `json:"cycles"`
	MarkerAckTimeoutMillis        *int64   `json:"markerAckTimeoutMillis"`
	KillTimeoutMillis             *int64   `json:"killTimeoutMillis"`
	DownMillis                    *int64   `json:"downMillis"`
	ReadyTimeoutMillis            *int64   `json:"readyTimeoutMillis"`
	ResyncTimeoutMillis           *int64   `json:"resyncTimeoutMillis"`
	InterRoundSettleMillis        *int64   `json:"interRoundSettleMillis"`
	ReconnectTimeoutMillis        *int64   `json:"reconnectTimeoutMillis"`
	RecoveryTimeoutMillis         *int64   `json:"recoveryTimeoutMillis"`
	HealthPollMillis              *int64   `json:"healthPollMillis"`
	SettleMillis                  *int64   `json:"settleMillis"`
	MaxMessages                   *uint64  `json:"maxMessages"`
	BrokerCPUs                    *float64 `json:"brokerCPUs"`
	BrokerMemoryBytes             *int64   `json:"brokerMemoryBytes"`
	GeneratorCPUs                 *float64 `json:"generatorCPUs"`
	GeneratorMemoryBytes          *int64   `json:"generatorMemoryBytes"`
}

// DecodeProfile requires every field and rejects unknown fields and trailing JSON.
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
			return Profile{}, errors.New("multiple JSON values")
		}
		return Profile{}, fmt.Errorf("decode profile trailer: %w", err)
	}
	for _, field := range []struct {
		name    string
		missing bool
	}{
		{"profileId", raw.ProfileID == nil},
		{"purpose", raw.Purpose == nil},
		{"seed", raw.Seed == nil},
		{"payloadBytes", raw.PayloadBytes == nil},
		{"publishRatePerStreamPerSecond", raw.PublishRatePerStreamPerSecond == nil},
		{"maxInflightPerPublisher", raw.MaxInflightPerPublisher == nil},
		{"ackTimeoutMillis", raw.AckTimeoutMillis == nil},
		{"sessionExpirySeconds", raw.SessionExpirySeconds == nil},
		{"keepAliveSeconds", raw.KeepAliveSeconds == nil},
		{"idleClients", raw.IdleClients == nil},
		{"warmupMillis", raw.WarmupMillis == nil},
		{"steadyMillis", raw.SteadyMillis == nil},
		{"killOrder", raw.KillOrder == nil},
		{"cycles", raw.Cycles == nil},
		{"markerAckTimeoutMillis", raw.MarkerAckTimeoutMillis == nil},
		{"killTimeoutMillis", raw.KillTimeoutMillis == nil},
		{"downMillis", raw.DownMillis == nil},
		{"readyTimeoutMillis", raw.ReadyTimeoutMillis == nil},
		{"resyncTimeoutMillis", raw.ResyncTimeoutMillis == nil},
		{"interRoundSettleMillis", raw.InterRoundSettleMillis == nil},
		{"reconnectTimeoutMillis", raw.ReconnectTimeoutMillis == nil},
		{"recoveryTimeoutMillis", raw.RecoveryTimeoutMillis == nil},
		{"healthPollMillis", raw.HealthPollMillis == nil},
		{"settleMillis", raw.SettleMillis == nil},
		{"maxMessages", raw.MaxMessages == nil},
		{"brokerCPUs", raw.BrokerCPUs == nil},
		{"brokerMemoryBytes", raw.BrokerMemoryBytes == nil},
		{"generatorCPUs", raw.GeneratorCPUs == nil},
		{"generatorMemoryBytes", raw.GeneratorMemoryBytes == nil},
	} {
		if field.missing {
			return Profile{}, fmt.Errorf("profile field %s is required", field.name)
		}
	}
	p := Profile{
		ProfileID: *raw.ProfileID, Purpose: *raw.Purpose, Seed: *raw.Seed,
		PayloadBytes: *raw.PayloadBytes, PublishRatePerStreamPerSecond: *raw.PublishRatePerStreamPerSecond,
		MaxInflightPerPublisher: *raw.MaxInflightPerPublisher, AckTimeoutMillis: *raw.AckTimeoutMillis,
		SessionExpirySeconds: *raw.SessionExpirySeconds, KeepAliveSeconds: *raw.KeepAliveSeconds, IdleClients: *raw.IdleClients,
		WarmupMillis: *raw.WarmupMillis, SteadyMillis: *raw.SteadyMillis, KillOrder: *raw.KillOrder, Cycles: *raw.Cycles,
		MarkerAckTimeoutMillis: *raw.MarkerAckTimeoutMillis, KillTimeoutMillis: *raw.KillTimeoutMillis, DownMillis: *raw.DownMillis,
		ReadyTimeoutMillis: *raw.ReadyTimeoutMillis, ResyncTimeoutMillis: *raw.ResyncTimeoutMillis,
		InterRoundSettleMillis: *raw.InterRoundSettleMillis, ReconnectTimeoutMillis: *raw.ReconnectTimeoutMillis, RecoveryTimeoutMillis: *raw.RecoveryTimeoutMillis,
		HealthPollMillis: *raw.HealthPollMillis, SettleMillis: *raw.SettleMillis, MaxMessages: *raw.MaxMessages,
		BrokerCPUs: *raw.BrokerCPUs, BrokerMemoryBytes: *raw.BrokerMemoryBytes, GeneratorCPUs: *raw.GeneratorCPUs, GeneratorMemoryBytes: *raw.GeneratorMemoryBytes,
	}
	if err := p.Validate(); err != nil {
		return Profile{}, err
	}
	return p, nil
}

// LoadProfile returns the validated profile, its original bytes and SHA-256.
func LoadProfile(path string) (Profile, []byte, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, nil, "", fmt.Errorf("read profile: %w", err)
	}
	p, err := DecodeProfile(data)
	if err != nil {
		return Profile{}, nil, "", err
	}
	digest := sha256.Sum256(data)
	return p, data, hex.EncodeToString(digest[:]), nil
}
