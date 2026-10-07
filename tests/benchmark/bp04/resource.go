package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

type resourceSummary struct {
	SchemaVersion         int              `json:"schemaVersion"`
	Target                string           `json:"target"`
	ClockSource           string           `json:"clockSource"`
	Valid                 bool             `json:"valid"`
	FailureReasons        []string         `json:"failureReasons"`
	NetworkStatsAvailable bool             `json:"networkStatsAvailable"`
	NetworkStatsReason    string           `json:"networkStatsReason,omitempty"`
	Windows               []resourceWindow `json:"windows"`
	Memory                memorySummary    `json:"memory"`
}

type resourceWindow struct {
	Window     string                `json:"window"`
	StartedAt  string                `json:"startedAt"`
	FinishedAt string                `json:"finishedAt"`
	Deliveries uint64                `json:"deliveries"`
	Nodes      []nodeResourceSummary `json:"nodes"`
	Cluster    resourceMeasurements  `json:"cluster"`
}

type nodeResourceSummary struct {
	Name string `json:"name"`
	resourceMeasurements
}

type resourceMeasurements struct {
	NetworkRxBytes    int64   `json:"networkRxBytes"`
	NetworkTxBytes    int64   `json:"networkTxBytes"`
	BlockReadBytes    int64   `json:"blockReadBytes"`
	BlockWriteBytes   int64   `json:"blockWriteBytes"`
	MedianCPUPercent  float64 `json:"medianCpuPercent"`
	MedianMemoryBytes int64   `json:"medianMemoryBytes"`
}

type memorySummary struct {
	IdleBaselineClusterMedianBytes   int64   `json:"idleBaselineClusterMedianBytes"`
	SubscribedIdleClusterMedianBytes int64   `json:"subscribedIdleClusterMedianBytes"`
	IncrementalMemoryBytes           int64   `json:"incrementalMemoryBytes"`
	SetupClients                     int     `json:"setupClients"`
	MemoryBytesPerSetupClient        float64 `json:"memoryBytesPerSetupClient"`
}

type resourcePhaseBounds struct {
	name       string
	step       int
	start, end time.Time
}

type nodeResourceStats struct {
	broker  []environment.StatsSample
	network []environment.StatsSample
}

func buildResourceSummary(target string, nodes []environment.ClusterNodeResult, phasePath string, workload workloadSummary) (resourceSummary, error) {
	summary := resourceSummary{
		SchemaVersion:         1,
		Target:                target,
		ClockSource:           "Docker stats and runner phases use host UTC timestamps",
		Valid:                 true,
		NetworkStatsAvailable: true,
	}
	if len(nodes) != bp04NodeCount {
		addResourceFailure(&summary, fmt.Sprintf("resource summary has %d nodes, want %d", len(nodes), bp04NodeCount))
	}
	phases, err := readResourcePhases(phasePath)
	if err != nil {
		return summary, err
	}
	expected := make(map[string]int, len(workload.Steps)+2)
	expected["idle-baseline"] = 0
	expected["subscribed-idle"] = 0
	for _, step := range workload.Steps {
		expected[fmt.Sprintf("step-%02d-measure", step.Step)] = step.Step
	}
	phaseByName := make(map[string]resourcePhaseBounds, len(phases))
	for _, phase := range phases {
		if _, wanted := expected[phase.name]; !wanted {
			continue
		}
		if _, duplicate := phaseByName[phase.name]; duplicate {
			addResourceFailure(&summary, fmt.Sprintf("duplicate resource phase %s", phase.name))
			continue
		}
		phaseByName[phase.name] = phase
	}
	for name := range expected {
		if _, ok := phaseByName[name]; !ok {
			addResourceFailure(&summary, fmt.Sprintf("missing phase window %s", name))
		}
	}
	statsByNode := make([]nodeResourceStats, len(nodes))
	for index, node := range nodes {
		broker, brokerErr := environment.ReadStats(node.BrokerStatsPath)
		if brokerErr != nil {
			addResourceFailure(&summary, fmt.Sprintf("%s broker stats: %v", node.Name, brokerErr))
		} else {
			statsByNode[index].broker = broker
		}
		network, networkErr := environment.ReadStats(node.NetworkStatsPath)
		if networkErr != nil {
			summary.NetworkStatsAvailable = false
			summary.NetworkStatsReason = "one or more network-owner stats files could not be read"
			addResourceFailure(&summary, fmt.Sprintf("%s network-owner stats: %v", node.Name, networkErr))
		} else {
			statsByNode[index].network = network
		}
	}
	for _, phase := range phases {
		if _, wanted := expected[phase.name]; !wanted {
			continue
		}
		window := resourceWindow{
			Window:     phase.name,
			StartedAt:  phase.start.UTC().Format(time.RFC3339Nano),
			FinishedAt: phase.end.UTC().Format(time.RFC3339Nano),
			Nodes:      make([]nodeResourceSummary, 0, len(nodes)),
		}
		if phase.step != 0 {
			window.Deliveries = stepDeliveryCount(workload, phase.step)
		}
		for index, node := range nodes {
			entry := nodeResourceSummary{Name: node.Name}
			brokerSamples, brokerErr := samplesInWindow(statsByNode[index].broker, phase.start, phase.end)
			if brokerErr != nil {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s broker stats: %v", phase.name, node.Name, brokerErr))
			} else if len(brokerSamples) < 2 {
				addResourceFailure(&summary, fmt.Sprintf("%s/%s has fewer than two broker stats samples", phase.name, node.Name))
			} else {
				entry.MedianCPUPercent = medianCPUPercent(brokerSamples)
				entry.MedianMemoryBytes = medianMemoryBytes(brokerSamples)
				entry.BlockReadBytes = brokerSamples[len(brokerSamples)-1].BlockReadBytes - brokerSamples[0].BlockReadBytes
				entry.BlockWriteBytes = brokerSamples[len(brokerSamples)-1].BlockWriteBytes - brokerSamples[0].BlockWriteBytes
				if entry.BlockReadBytes < 0 || entry.BlockWriteBytes < 0 {
					addResourceFailure(&summary, fmt.Sprintf("%s/%s broker block counters decreased", phase.name, node.Name))
				}
			}
			networkSamples, networkErr := samplesInWindow(statsByNode[index].network, phase.start, phase.end)
			if networkErr != nil {
				summary.NetworkStatsAvailable = false
				addResourceFailure(&summary, fmt.Sprintf("%s/%s network-owner stats: %v", phase.name, node.Name, networkErr))
			} else if len(networkSamples) < 2 {
				summary.NetworkStatsAvailable = false
				addResourceFailure(&summary, fmt.Sprintf("%s/%s has fewer than two network-owner stats samples", phase.name, node.Name))
			} else {
				entry.NetworkRxBytes = networkSamples[len(networkSamples)-1].NetworkRxBytes - networkSamples[0].NetworkRxBytes
				entry.NetworkTxBytes = networkSamples[len(networkSamples)-1].NetworkTxBytes - networkSamples[0].NetworkTxBytes
				if entry.NetworkRxBytes < 0 || entry.NetworkTxBytes < 0 {
					addResourceFailure(&summary, fmt.Sprintf("%s/%s network counters decreased", phase.name, node.Name))
				}
			}
			addResourceMeasurements(&window.Cluster, entry.resourceMeasurements)
			window.Nodes = append(window.Nodes, entry)
		}
		summary.Windows = append(summary.Windows, window)
	}
	idle, idleOK := resourceWindowByName(summary.Windows, "idle-baseline")
	subscribed, subscribedOK := resourceWindowByName(summary.Windows, "subscribed-idle")
	if idleOK && subscribedOK {
		delta := subscribed.Cluster.MedianMemoryBytes - idle.Cluster.MedianMemoryBytes
		summary.Memory = memorySummary{
			IdleBaselineClusterMedianBytes:   idle.Cluster.MedianMemoryBytes,
			SubscribedIdleClusterMedianBytes: subscribed.Cluster.MedianMemoryBytes,
			IncrementalMemoryBytes:           delta,
			SetupClients:                     workload.Setup.Clients,
		}
		if workload.Setup.Clients > 0 {
			summary.Memory.MemoryBytesPerSetupClient = float64(delta) / float64(workload.Setup.Clients)
		} else {
			addResourceFailure(&summary, "setup client count is zero")
		}
	}
	if !summary.NetworkStatsAvailable && summary.NetworkStatsReason == "" {
		summary.NetworkStatsReason = "network-owner stats did not cover every required window"
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

func readResourcePhases(path string) ([]resourcePhaseBounds, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open phases %q: %w", path, err)
	}
	defer file.Close()
	active := make(map[string]time.Time)
	completed := make(map[string]struct{})
	var phases []resourcePhaseBounds
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1<<20)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		var event phaseEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("decode phase line %d: %w", lineNumber, err)
		}
		if !isResourcePhase(event.Phase) {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, event.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse phase timestamp on line %d: %w", lineNumber, err)
		}
		switch event.Event {
		case "start":
			if _, exists := active[event.Phase]; exists {
				return nil, fmt.Errorf("phase %q has more than one start", event.Phase)
			}
			if _, exists := completed[event.Phase]; exists {
				return nil, fmt.Errorf("phase %q is repeated", event.Phase)
			}
			active[event.Phase] = at
		case "end":
			start, exists := active[event.Phase]
			if !exists {
				return nil, fmt.Errorf("phase %q ends without a start", event.Phase)
			}
			if at.Before(start) {
				return nil, fmt.Errorf("phase %q ends before it starts", event.Phase)
			}
			step, err := resourceStep(event.Phase)
			if err != nil {
				return nil, err
			}
			phases = append(phases, resourcePhaseBounds{name: event.Phase, step: step, start: start, end: at})
			delete(active, event.Phase)
			completed[event.Phase] = struct{}{}
		default:
			return nil, fmt.Errorf("unknown phase event %q on line %d", event.Event, lineNumber)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read phases %q: %w", path, err)
	}
	if len(active) != 0 {
		for phase := range active {
			return nil, fmt.Errorf("phase %q has no end event", phase)
		}
	}
	sort.Slice(phases, func(i, j int) bool { return phases[i].start.Before(phases[j].start) })
	return phases, nil
}

func isResourcePhase(phase string) bool {
	return phase == "idle-baseline" || phase == "subscribed-idle" || (strings.HasPrefix(phase, "step-") && strings.HasSuffix(phase, "-measure"))
}

func resourceStep(phase string) (int, error) {
	if phase == "idle-baseline" || phase == "subscribed-idle" {
		return 0, nil
	}
	value := strings.TrimSuffix(strings.TrimPrefix(phase, "step-"), "-measure")
	step, err := strconv.Atoi(value)
	if err != nil || step < 1 {
		return 0, fmt.Errorf("invalid measurement phase %q", phase)
	}
	return step, nil
}

func samplesInWindow(samples []environment.StatsSample, start, end time.Time) ([]environment.StatsSample, error) {
	selected := make([]environment.StatsSample, 0)
	for _, sample := range samples {
		at, err := time.Parse(time.RFC3339Nano, sample.CollectedAt)
		if err != nil {
			return nil, fmt.Errorf("invalid stats timestamp %q: %w", sample.CollectedAt, err)
		}
		if !at.Before(start) && !at.After(end) {
			selected = append(selected, sample)
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		left, _ := time.Parse(time.RFC3339Nano, selected[i].CollectedAt)
		right, _ := time.Parse(time.RFC3339Nano, selected[j].CollectedAt)
		return left.Before(right)
	})
	return selected, nil
}

func medianCPUPercent(samples []environment.StatsSample) float64 {
	values := make([]float64, len(samples))
	for index, sample := range samples {
		values[index] = sample.CPUPercent
	}
	sort.Float64s(values)
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
}

func medianMemoryBytes(samples []environment.StatsSample) int64 {
	values := make([]int64, len(samples))
	for index, sample := range samples {
		values[index] = sample.MemoryUsageBytes
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	lower := values[middle-1]
	upper := values[middle]
	return lower + (upper-lower)/2
}

func addResourceMeasurements(total *resourceMeasurements, value resourceMeasurements) {
	total.NetworkRxBytes += value.NetworkRxBytes
	total.NetworkTxBytes += value.NetworkTxBytes
	total.BlockReadBytes += value.BlockReadBytes
	total.BlockWriteBytes += value.BlockWriteBytes
	total.MedianCPUPercent += value.MedianCPUPercent
	total.MedianMemoryBytes += value.MedianMemoryBytes
}

func resourceWindowByName(windows []resourceWindow, name string) (resourceWindow, bool) {
	for _, window := range windows {
		if window.Window == name {
			return window, true
		}
	}
	return resourceWindow{}, false
}

func stepDeliveryCount(workload workloadSummary, stepNumber int) uint64 {
	for _, step := range workload.Steps {
		if step.Step != stepNumber {
			continue
		}
		var deliveries uint64
		for _, class := range step.Classes {
			deliveries += class.Delivered
		}
		return deliveries
	}
	return 0
}

func addResourceFailure(summary *resourceSummary, reason string) {
	summary.Valid = false
	summary.FailureReasons = append(summary.FailureReasons, reason)
}
