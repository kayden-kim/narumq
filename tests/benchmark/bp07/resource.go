package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

const resourceClockSource = "stats=host clock, phases=runner container clock"

type resourceWindow struct {
	Window  string                `json:"window"`
	Nodes   []nodeResourceSummary `json:"nodes"`
	Cluster resourceMeasurements  `json:"cluster"`
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

func buildResourceSummary(target string, nodes []environment.ClusterNodeResult, phasePath string) (resourceSummary, error) {
	summary := resourceSummary{
		SchemaVersion:         1,
		Target:                target,
		ClockSource:           resourceClockSource,
		Valid:                 true,
		FailureReasons:        []string{},
		NetworkStatsAvailable: true,
		Windows:               []resourceWindow{},
	}
	phase, found, err := readRollingPhase(phasePath)
	if err != nil {
		return summary, err
	}
	if !found {
		addResourceFailure(&summary, "missing phase window rolling")
	}
	if len(nodes) == 0 {
		addResourceFailure(&summary, "rolling has no cluster nodes")
	}

	networkNonzero := false
	if found {
		window := resourceWindow{Window: phase.name, Nodes: make([]nodeResourceSummary, 0, len(nodes))}
		for _, node := range nodes {
			broker, brokerErr := environment.ReadStats(node.BrokerStatsPath)
			network, networkErr := environment.ReadStats(node.NetworkStatsPath)
			if brokerErr != nil {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s broker stats: %v", phase.name, node.Name, brokerErr))
			}
			if networkErr != nil {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s network stats: %v", phase.name, node.Name, networkErr))
			}

			brokerSamples := selectResourceStats(broker, phase.start, phase.end, true)
			networkSamples := selectResourceStats(network, phase.start, phase.end, false)
			brokerOK := len(brokerSamples) >= 2
			networkOK := len(networkSamples) >= 2
			if !brokerOK {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s has fewer than two running broker stats samples", phase.name, node.Name))
			}
			if !networkOK {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s has fewer than two network-owner stats samples", phase.name, node.Name))
			}

			entry := nodeResourceSummary{Name: node.Name}
			if brokerOK {
				entry.BlockReadBytes = resetAwareBlockReadBytes(brokerSamples)
				entry.BlockWriteBytes = resetAwareBlockWriteBytes(brokerSamples)
				entry.MedianCPUPercent = medianResourceCPU(brokerSamples)
				for _, sample := range brokerSamples {
					if sample.sample.MemoryUsageBytes > entry.MaxMemoryBytes {
						entry.MaxMemoryBytes = sample.sample.MemoryUsageBytes
					}
				}
			}
			if networkOK {
				first := networkSamples[0].sample
				last := networkSamples[len(networkSamples)-1].sample
				entry.NetworkRxBytes = last.NetworkRxBytes - first.NetworkRxBytes
				entry.NetworkTxBytes = last.NetworkTxBytes - first.NetworkTxBytes
				if entry.NetworkRxBytes != 0 || entry.NetworkTxBytes != 0 {
					networkNonzero = true
				}
			}
			addResourceMeasurements(&window.Cluster, entry.resourceMeasurements)
			window.Nodes = append(window.Nodes, entry)
		}
		summary.Windows = append(summary.Windows, window)
	}
	if !networkNonzero {
		summary.NetworkStatsAvailable = false
		summary.NetworkStatsReason = "all network-owner network Rx/Tx deltas across windows are zero"
	}
	return summary, nil
}

func writeResourceSummary(artifactDir string, summary resourceSummary) error {
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(artifactDir, "resource-summary.json"), append(data, '\n'), 0o644)
}

type resourcePhaseBounds struct {
	name       string
	start, end time.Time
}

type resourcePhaseEvent struct {
	Phase     string `json:"phase"`
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}

func readRollingPhase(path string) (resourcePhaseBounds, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return resourcePhaseBounds{}, false, fmt.Errorf("open phases %q: %w", path, err)
	}
	defer file.Close()

	var start time.Time
	active := false
	var phases []resourcePhaseBounds
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1<<20)
	line := 0
	for scanner.Scan() {
		line++
		var event resourcePhaseEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return resourcePhaseBounds{}, false, fmt.Errorf("decode phase line %d: %w", line, err)
		}
		if event.Phase != "rolling" {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, event.Timestamp)
		if err != nil {
			return resourcePhaseBounds{}, false, fmt.Errorf("parse phase timestamp on line %d: %w", line, err)
		}
		switch event.Event {
		case "start":
			if active {
				return resourcePhaseBounds{}, false, fmt.Errorf("phase %q starts before it ends", event.Phase)
			}
			start = at
			active = true
		case "end":
			if !active {
				return resourcePhaseBounds{}, false, fmt.Errorf("phase %q ends without a start", event.Phase)
			}
			if at.Before(start) {
				return resourcePhaseBounds{}, false, fmt.Errorf("phase %q ends before it starts", event.Phase)
			}
			phases = append(phases, resourcePhaseBounds{name: event.Phase, start: start, end: at})
			active = false
		default:
			return resourcePhaseBounds{}, false, fmt.Errorf("unknown phase event %q on line %d", event.Event, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return resourcePhaseBounds{}, false, fmt.Errorf("read phases %q: %w", path, err)
	}
	if active {
		return resourcePhaseBounds{}, false, fmt.Errorf("phase %q has no end event", "rolling")
	}
	if len(phases) > 1 {
		return resourcePhaseBounds{}, false, fmt.Errorf("phase %q has multiple windows", phases[0].name)
	}
	if len(phases) == 0 {
		return resourcePhaseBounds{}, false, nil
	}
	return phases[0], true, nil
}

type timestampedResourceStat struct {
	sample environment.StatsSample
	at     time.Time
}

func selectResourceStats(samples []environment.StatsSample, start, end time.Time, runningOnly bool) []timestampedResourceStat {
	selected := make([]timestampedResourceStat, 0, len(samples))
	for _, sample := range samples {
		at, err := time.Parse(time.RFC3339Nano, sample.CollectedAt)
		if err != nil || at.Before(start) || at.After(end) {
			continue
		}
		if runningOnly && sample.MemoryUsageBytes <= 0 {
			continue
		}
		selected = append(selected, timestampedResourceStat{sample: sample, at: at})
	}
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].at.Before(selected[j].at) })
	return selected
}

func medianResourceCPU(samples []timestampedResourceStat) float64 {
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = sample.sample.CPUPercent
	}
	sort.Float64s(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

func resetAwareBlockReadBytes(samples []timestampedResourceStat) int64 {
	var total int64
	for i := 1; i < len(samples); i++ {
		previous := samples[i-1].sample.BlockReadBytes
		current := samples[i].sample.BlockReadBytes
		if current >= previous {
			total += current - previous
		} else {
			total += current
		}
	}
	return total
}

func resetAwareBlockWriteBytes(samples []timestampedResourceStat) int64 {
	var total int64
	for i := 1; i < len(samples); i++ {
		previous := samples[i-1].sample.BlockWriteBytes
		current := samples[i].sample.BlockWriteBytes
		if current >= previous {
			total += current - previous
		} else {
			total += current
		}
	}
	return total
}

func addResourceMeasurements(total *resourceMeasurements, value resourceMeasurements) {
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
