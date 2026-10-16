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
	"reflect"
	"strings"
	"unicode"
)

const (
	purposeLocalSmoke  = "local-smoke"
	purposeEKSCapacity = "eks-capacity"

	// Cell geometry is fixed by §3.9: 8 publishers, 23 subscribers, 4 retained
	// subscribers and 18 idle clients.
	cellStreams = 8
	cellClients = 53
	idleTenantA = 12
	idleTenantB = 6

	maxProfileCells = 1024
)

// StepSpec is one offered-load step of the capacity ladder.
type StepSpec struct {
	Cells                  int `json:"cells"`
	RatePerStreamPerSecond int `json:"ratePerStreamPerSecond"`
}

// OfferedPerSecond returns the offered publish rate 8*cells*rate.
func (s StepSpec) OfferedPerSecond() int {
	return cellStreams * s.Cells * s.RatePerStreamPerSecond
}

// Profile is the strict BP-02 EKS capacity profile (§3.9).
type Profile struct {
	ProfileID                     string     `json:"profileId"`
	Purpose                       string     `json:"purpose"`
	Seed                          uint64     `json:"seed"`
	PayloadSizeCycle              []int      `json:"payloadSizeCycle"`
	KeepAliveSeconds              uint16     `json:"keepAliveSeconds"`
	SessionExpirySeconds          uint32     `json:"sessionExpirySeconds"`
	TLSEveryNthClient             int        `json:"tlsEveryNthClient"`
	RetainedTopicsTenantA         int        `json:"retainedTopicsTenantA"`
	RetainedTopicsTenantB         int        `json:"retainedTopicsTenantB"`
	MaxInflightPerPublisher       int        `json:"maxInflightPerPublisher"`
	AckTimeoutMillis              int64      `json:"ackTimeoutMillis"`
	Steps                         []StepSpec `json:"steps"`
	BisectRounds                  int        `json:"bisectRounds"`
	ConfirmRuns                   int        `json:"confirmRuns"`
	ConnectRatePerSecond          int        `json:"connectRatePerSecond"`
	WarmupMillis                  int64      `json:"warmupMillis"`
	MeasureMillis                 int64      `json:"measureMillis"`
	DrainMillis                   int64      `json:"drainMillis"`
	RecoveryTimeoutMillis         int64      `json:"recoveryTimeoutMillis"`
	ReadyTimeoutMillis            int64      `json:"readyTimeoutMillis"`
	ResyncTimeoutMillis           int64      `json:"resyncTimeoutMillis"`
	MarkerAckTimeoutMillis        int64      `json:"markerAckTimeoutMillis"`
	LatencyP99LimitMillis         int64      `json:"latencyP99LimitMillis"`
	GeneratorCPUSaturationPercent float64    `json:"generatorCpuSaturationPercent"`
	BrokerCPUs                    float64    `json:"brokerCPUs"`
	BrokerMemoryBytes             int64      `json:"brokerMemoryBytes"`
	GeneratorCPUs                 float64    `json:"generatorCPUs"`
	GeneratorMemoryBytes          int64      `json:"generatorMemoryBytes"`
}

// MaxCells returns the largest cell count any ladder step can use.
func (p Profile) MaxCells() int {
	maxCells := 0
	for _, step := range p.Steps {
		maxCells = max(maxCells, step.Cells)
	}
	return maxCells
}

// Validate checks every bound before any Docker, network or allocation work.
func (p Profile) Validate() error {
	if strings.TrimSpace(p.ProfileID) == "" || strings.IndexFunc(p.ProfileID, unicode.IsSpace) >= 0 {
		return errors.New("profileId is required and must not contain whitespace")
	}
	if p.Purpose != purposeLocalSmoke && p.Purpose != purposeEKSCapacity {
		return fmt.Errorf("purpose must be %q or %q", purposeLocalSmoke, purposeEKSCapacity)
	}
	if l := len(p.PayloadSizeCycle); l < 1 || l > 32 {
		return errors.New("payloadSizeCycle must contain 1 to 32 sizes")
	}
	for index, size := range p.PayloadSizeCycle {
		if size < 32 || size > 65536 {
			return fmt.Errorf("payloadSizeCycle[%d] must be between 32 and 65536 bytes", index)
		}
	}
	checks := []struct {
		name     string
		value    int64
		low, top int64
	}{
		{"keepAliveSeconds", int64(p.KeepAliveSeconds), 10, 300},
		{"sessionExpirySeconds", int64(p.SessionExpirySeconds), 60, 3600},
		{"tlsEveryNthClient", int64(p.TLSEveryNthClient), 1, 16},
		{"retainedTopicsTenantA", int64(p.RetainedTopicsTenantA), 1, 64},
		{"retainedTopicsTenantB", int64(p.RetainedTopicsTenantB), 1, 64},
		{"maxInflightPerPublisher", int64(p.MaxInflightPerPublisher), 1, 64},
		{"ackTimeoutMillis", p.AckTimeoutMillis, 1000, 60000},
		{"bisectRounds", int64(p.BisectRounds), 0, 4},
		{"confirmRuns", int64(p.ConfirmRuns), 1, 10},
		{"connectRatePerSecond", int64(p.ConnectRatePerSecond), 1, 5000},
		{"warmupMillis", p.WarmupMillis, 1000, 600000},
		{"measureMillis", p.MeasureMillis, 2000, 600000},
		{"drainMillis", p.DrainMillis, 1000, 120000},
		{"recoveryTimeoutMillis", p.RecoveryTimeoutMillis, 1000, 600000},
		{"readyTimeoutMillis", p.ReadyTimeoutMillis, 10000, 1800000},
		{"resyncTimeoutMillis", p.ResyncTimeoutMillis, 1000, 1800000},
		{"markerAckTimeoutMillis", p.MarkerAckTimeoutMillis, 1000, 60000},
		{"latencyP99LimitMillis", p.LatencyP99LimitMillis, 1, 10000},
		{"brokerMemoryBytes", p.BrokerMemoryBytes, 1, math.MaxInt64},
		{"generatorMemoryBytes", p.GeneratorMemoryBytes, 1, math.MaxInt64},
	}
	for _, check := range checks {
		if check.value < check.low || check.value > check.top {
			return fmt.Errorf("%s must be between %d and %d", check.name, check.low, check.top)
		}
	}
	for _, field := range []struct {
		name  string
		value float64
	}{{"brokerCPUs", p.BrokerCPUs}, {"generatorCPUs", p.GeneratorCPUs}} {
		if field.value <= 0 || math.IsNaN(field.value) || math.IsInf(field.value, 0) {
			return fmt.Errorf("%s must be a positive finite number", field.name)
		}
	}
	if p.GeneratorCPUSaturationPercent <= 0 || p.GeneratorCPUSaturationPercent > 100 || math.IsNaN(p.GeneratorCPUSaturationPercent) {
		return errors.New("generatorCpuSaturationPercent must be in (0, 100]")
	}
	if len(p.Steps) < 1 || len(p.Steps) > 16 {
		return errors.New("steps must contain 1 to 16 entries")
	}
	previous := 0
	for index, step := range p.Steps {
		if step.Cells < 1 || step.Cells > maxProfileCells {
			return fmt.Errorf("steps[%d].cells must be between 1 and %d", index, maxProfileCells)
		}
		if step.RatePerStreamPerSecond < 1 || step.RatePerStreamPerSecond > 50 {
			return fmt.Errorf("steps[%d].ratePerStreamPerSecond must be between 1 and 50", index)
		}
		if offered := step.OfferedPerSecond(); offered <= previous {
			return fmt.Errorf("steps[%d] offered load %d must exceed the previous step's %d", index, offered, previous)
		} else {
			previous = offered
		}
	}
	return nil
}

// DecodeProfile strictly decodes a profile: unknown, missing and trailing JSON are rejected.
func DecodeProfile(data []byte) (Profile, error) {
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(data, &keys); err != nil {
		return Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	profileType := reflect.TypeFor[Profile]()
	for index := range profileType.NumField() {
		field := profileType.Field(index)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if _, ok := keys[name]; !ok {
			return Profile{}, fmt.Errorf("profile field %q is required", name)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var profile Profile
	if err := decoder.Decode(&profile); err != nil {
		return Profile{}, fmt.Errorf("decode profile: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Profile{}, errors.New("decode profile: trailing JSON value")
	}
	var steps []map[string]json.RawMessage
	if err := json.Unmarshal(keys["steps"], &steps); err != nil {
		return Profile{}, fmt.Errorf("decode profile steps: %w", err)
	}
	for index, step := range steps {
		for _, name := range []string{"cells", "ratePerStreamPerSecond"} {
			if _, ok := step[name]; !ok {
				return Profile{}, fmt.Errorf("profile steps[%d] field %q is required", index, name)
			}
		}
	}
	if err := profile.Validate(); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

// LoadProfile reads a profile file and returns it with the SHA-256 of its source bytes.
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
