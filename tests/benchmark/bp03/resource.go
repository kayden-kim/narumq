package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

const resourceClockSource = "stats=host clock, phases=runner container clock"

type resourceWindow struct {
	Window                         string                `json:"window"`
	AckedMessages                  uint64                `json:"ackedMessages"`
	Nodes                          []nodeResourceSummary `json:"nodes"`
	Cluster                        resourceMeasurements  `json:"cluster"`
	NetworkTxBytesPerAckedMessage  *float64              `json:"networkTxBytesPerAckedMessage,omitempty"`
	BlockWriteBytesPerAckedMessage *float64              `json:"blockWriteBytesPerAckedMessage,omitempty"`
}

type nodeResourceSummary struct {
	Name string `json:"name"`
	resourceMeasurements
}

type resourceMeasurements struct {
	NetworkRxBytes   int64   `json:"networkRxBytes"`
	NetworkTxBytes   int64   `json:"networkTxBytes"`
	BlockReadBytes   int64   `json:"blockReadBytes"`
	BlockWriteBytes  int64   `json:"blockWriteBytes"`
	MedianCPUPercent float64 `json:"medianCpuPercent"`
	MaxMemoryBytes   int64   `json:"maxMemoryUsageBytes"`
}

type resourceSummary struct {
	SchemaVersion         int              `json:"schemaVersion"`
	Target                string           `json:"target"`
	ClockSource           string           `json:"clockSource"`
	Valid                 bool             `json:"valid"`
	FailureReasons        []string         `json:"failureReasons"`
	NetworkStatsAvailable bool             `json:"networkStatsAvailable"`
	NetworkStatsReason    string           `json:"networkStatsReason,omitempty"`
	Windows               []resourceWindow `json:"windows"`
}

func buildResourceSummary(target string, nodes []environment.ClusterNodeResult, phasePath string, workload workloadSummary) (resourceSummary, error) {
	summary := resourceSummary{
		SchemaVersion:         1,
		Target:                target,
		ClockSource:           resourceClockSource,
		Valid:                 true,
		NetworkStatsAvailable: true,
	}
	windows, err := readResourcePhases(phasePath)
	if err != nil {
		return summary, err
	}
	foundWindows := make(map[string]bool, len(windows))
	for _, window := range windows {
		foundWindows[window.name] = true
	}
	for _, step := range workload.Steps {
		name := fmt.Sprintf("step-%02d-measure", step.Step)
		if !foundWindows[name] {
			addResourceFailure(&summary, fmt.Sprintf("missing phase window %s", name))
		}
	}
	if !foundWindows["offline"] {
		addResourceFailure(&summary, "missing phase window offline")
	}

	stepAcked := make(map[int]uint64, len(workload.Steps))
	for _, step := range workload.Steps {
		for _, qos := range step.QoS {
			if qos.Acked > 0 {
				stepAcked[step.Step] += uint64(qos.Acked)
			}
		}
	}
	var offlineAcked uint64
	for _, qos := range workload.Offline.QoS {
		if qos.Acked > 0 {
			offlineAcked += uint64(qos.Acked)
		}
	}
	networkNonzero := false
	for _, window := range windows {
		result := resourceWindow{Window: window.name, Nodes: make([]nodeResourceSummary, 0, len(nodes))}
		if window.name == "offline" {
			result.AckedMessages = offlineAcked
		} else {
			result.AckedMessages = stepAcked[window.step]
		}
		for _, node := range nodes {
			broker, brokerErr := environment.ReadStats(node.BrokerStatsPath)
			network, networkErr := environment.ReadStats(node.NetworkStatsPath)
			if brokerErr != nil {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s broker stats: %v", window.name, node.Name, brokerErr))
			}
			if networkErr != nil {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s network stats: %v", window.name, node.Name, networkErr))
			}
			brokerWindow, brokerOK := selectStatsWindow(broker, window.start, window.end)
			networkWindow, networkOK := selectStatsWindow(network, window.start, window.end)
			if !brokerOK {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s has fewer than two broker stats samples", window.name, node.Name))
			}
			if !networkOK {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s has fewer than two network-owner stats samples", window.name, node.Name))
			}
			entry := nodeResourceSummary{Name: node.Name}
			if brokerOK {
				entry.BlockReadBytes = brokerWindow.last.BlockReadBytes - brokerWindow.first.BlockReadBytes
				entry.BlockWriteBytes = brokerWindow.last.BlockWriteBytes - brokerWindow.first.BlockWriteBytes
				entry.MedianCPUPercent = medianCPU(brokerWindow.samples)
				for _, sample := range brokerWindow.samples {
					if sample.MemoryUsageBytes > entry.MaxMemoryBytes {
						entry.MaxMemoryBytes = sample.MemoryUsageBytes
					}
				}
			}
			if networkOK {
				entry.NetworkRxBytes = networkWindow.last.NetworkRxBytes - networkWindow.first.NetworkRxBytes
				entry.NetworkTxBytes = networkWindow.last.NetworkTxBytes - networkWindow.first.NetworkTxBytes
				if entry.NetworkRxBytes != 0 || entry.NetworkTxBytes != 0 {
					networkNonzero = true
				}
			}
			addMeasurements(&result.Cluster, entry.resourceMeasurements)
			result.Nodes = append(result.Nodes, entry)
		}
		if result.AckedMessages > 0 {
			networkPerMessage := float64(result.Cluster.NetworkTxBytes) / float64(result.AckedMessages)
			blockPerMessage := float64(result.Cluster.BlockWriteBytes) / float64(result.AckedMessages)
			result.NetworkTxBytesPerAckedMessage = &networkPerMessage
			result.BlockWriteBytesPerAckedMessage = &blockPerMessage
		}
		summary.Windows = append(summary.Windows, result)
	}
	if !networkNonzero {
		summary.NetworkStatsAvailable = false
		summary.NetworkStatsReason = "all network-owner network Rx/Tx deltas across windows are zero"
		for i := range summary.Windows {
			summary.Windows[i].NetworkTxBytesPerAckedMessage = nil
		}
	}
	return summary, nil
}

func writeResourceSummary(outputDir string, summary resourceSummary) error {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outputDir, "resource-summary.json"), append(data, '\n'), 0o644)
}

type phaseBounds struct {
	name       string
	step       int
	start, end time.Time
}
type phaseEventRecord struct {
	Phase     string `json:"phase"`
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}

func readResourcePhases(path string) ([]phaseBounds, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open phases %q: %w", path, err)
	}
	defer file.Close()
	active := make(map[string]time.Time)
	var result []phaseBounds
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1<<20)
	line := 0
	for scanner.Scan() {
		line++
		var event phaseEventRecord
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("decode phase line %d: %w", line, err)
		}
		if !resourcePhaseWanted(event.Phase) {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, event.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse phase timestamp on line %d: %w", line, err)
		}
		switch event.Event {
		case "start":
			active[event.Phase] = at
		case "end":
			start, ok := active[event.Phase]
			if !ok {
				return nil, fmt.Errorf("phase %q ends without a start", event.Phase)
			}
			if at.Before(start) {
				return nil, fmt.Errorf("phase %q ends before it starts", event.Phase)
			}
			step := 0
			if event.Phase != "offline" {
				if _, err := fmt.Sscanf(event.Phase, "step-%02d-measure", &step); err != nil {
					return nil, fmt.Errorf("invalid measure phase %q", event.Phase)
				}
			}
			result = append(result, phaseBounds{name: event.Phase, step: step, start: start, end: at})
			delete(active, event.Phase)
		default:
			return nil, fmt.Errorf("unknown phase event %q on line %d", event.Event, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read phases %q: %w", path, err)
	}
	if len(active) != 0 {
		return nil, fmt.Errorf("phase %q has no end event", firstMapKey(active))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].start.Before(result[j].start) })
	return result, nil
}

func resourcePhaseWanted(phase string) bool {
	return phase == "offline" || (strings.HasPrefix(phase, "step-") && strings.HasSuffix(phase, "-measure"))
}
func firstMapKey(values map[string]time.Time) string {
	for key := range values {
		return key
	}
	return ""
}

type selectedStats struct {
	samples   []environment.StatsSample
	first     environment.StatsSample
	last      environment.StatsSample
	firstTime time.Time
	lastTime  time.Time
}

func selectStatsWindow(samples []environment.StatsSample, start, end time.Time) (selectedStats, bool) {
	var selected selectedStats
	for _, sample := range samples {
		at, err := time.Parse(time.RFC3339Nano, sample.CollectedAt)
		if err != nil || at.Before(start) || at.After(end) {
			continue
		}
		selected.samples = append(selected.samples, sample)
		if len(selected.samples) == 1 || at.Before(selected.firstTime) {
			selected.first = sample
			selected.firstTime = at
		}
		if len(selected.samples) == 1 || at.After(selected.lastTime) {
			selected.last = sample
			selected.lastTime = at
		}
	}
	if len(selected.samples) < 2 {
		return selected, false
	}
	return selected, true
}
func medianCPU(samples []environment.StatsSample) float64 {
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = sample.CPUPercent
	}
	sort.Float64s(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}
func addMeasurements(total *resourceMeasurements, value resourceMeasurements) {
	total.NetworkRxBytes += value.NetworkRxBytes
	total.NetworkTxBytes += value.NetworkTxBytes
	total.BlockReadBytes += value.BlockReadBytes
	total.BlockWriteBytes += value.BlockWriteBytes
	total.MedianCPUPercent += value.MedianCPUPercent
	if value.MaxMemoryBytes > total.MaxMemoryBytes {
		total.MaxMemoryBytes = value.MaxMemoryBytes
	}
}
func addResourceFailure(summary *resourceSummary, reason string) {
	summary.Valid = false
	summary.FailureReasons = append(summary.FailureReasons, reason)
}
