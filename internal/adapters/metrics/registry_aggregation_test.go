package metrics

import (
	"bufio"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	ports "narumq/internal/application/ports/metrics"
)

var aggregationMetricNamePattern = regexp.MustCompile(`^narumq_[a-z0-9_]+$`)
var expositionSamplePattern = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? (\S+)$`)

type aggregationCatalogEntry = ports.MetricDescriptorDTO

func loadAggregationCatalog(t *testing.T) map[string]aggregationCatalogEntry {
	t.Helper()
	catalog := make(map[string]aggregationCatalogEntry)
	for _, descriptor := range ports.MetricCatalog() {
		entry := descriptor
		if !aggregationMetricNamePattern.MatchString(entry.Name) {
			t.Errorf("%s: metric name must match ^narumq_[a-z0-9_]+$", entry.Name)
		}
		if _, exists := catalog[entry.Name]; exists {
			t.Errorf("%s: duplicate metric family", entry.Name)
		}
		switch entry.Type {
		case ports.MetricTypeCounter, ports.MetricTypeGauge, ports.MetricTypeHistogram:
		default:
			t.Errorf("%s: unknown metric type %q", entry.Name, entry.Type)
		}
		switch entry.Scope {
		case ports.MetricScopeNode, ports.MetricScopeReplicated, ports.MetricScopeLeader:
		default:
			t.Errorf("%s: unknown metric scope %q", entry.Name, entry.Scope)
		}
		switch entry.Aggregation {
		case ports.MetricAggregationIncreaseSum, ports.MetricAggregationSum, ports.MetricAggregationMax,
			ports.MetricAggregationPerNode, ports.MetricAggregationBucketSum:
		default:
			t.Errorf("%s: unknown cluster aggregation %q", entry.Name, entry.Aggregation)
		}

		switch entry.Type {
		case ports.MetricTypeCounter:
			if !strings.HasSuffix(entry.Name, "_total") {
				t.Errorf("%s: counter names must end in _total", entry.Name)
			}
			if entry.Scope != ports.MetricScopeNode || entry.Aggregation != ports.MetricAggregationIncreaseSum {
				t.Errorf("%s: counter requires scope node and aggregation increase_sum", entry.Name)
			}
		case ports.MetricTypeHistogram:
			if entry.Scope != ports.MetricScopeNode || entry.Aggregation != ports.MetricAggregationBucketSum {
				t.Errorf("%s: histogram requires scope node and aggregation bucket_sum", entry.Name)
			}
			if strings.HasSuffix(entry.Name, "_total") {
				t.Errorf("%s: histogram names must not end in _total", entry.Name)
			}
			if containsString(entry.LabelNames, "le") {
				t.Errorf("%s: histogram catalog labels must not include le", entry.Name)
			}
		case ports.MetricTypeGauge:
			if entry.Aggregation != ports.MetricAggregationSum && entry.Aggregation != ports.MetricAggregationMax &&
				entry.Aggregation != ports.MetricAggregationPerNode {
				t.Errorf("%s: gauge aggregation must be sum, max, or per_node", entry.Name)
			}
			if strings.HasSuffix(entry.Name, "_total") {
				t.Errorf("%s: gauge names must not end in _total", entry.Name)
			}
		}
		if entry.Aggregation == ports.MetricAggregationSum && entry.Scope != ports.MetricScopeNode {
			t.Errorf("%s: sum requires scope node", entry.Name)
		}
		if entry.Scope == ports.MetricScopeLeader && entry.Aggregation != ports.MetricAggregationMax {
			t.Errorf("%s: leader scope requires aggregation max", entry.Name)
		}
		catalog[entry.Name] = entry
	}
	return catalog
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func fullyObservedRegistry() *Registry {
	registry := NewRegistry()
	registry.ConnectionOpened(ports.ProtocolMQTT50, ports.TransportTCP)
	registry.PacketReceived("PUBLISH")
	registry.PacketSent("PUBACK")
	registry.AuthAttempt(ports.AuthMethodPassword, true, "none")
	registry.ConnectRejected(ports.ProtocolMQTT310, ports.ConnectRejectReasonUnsupportedProtocolVersion)
	registry.ConnectRejected(ports.ProtocolMQTT50, ports.ConnectRejectReasonNotAuthorized)
	registry.NodeDrainTransition("COMPLETED")
	registry.MessageDropped(ports.MessageDropReasonPacketTooLarge)
	registry.SetNodeDrainProgress(true, NodeDrainProgress{ConnectionsInitial: 1})
	registry.RecordTenantQuotaRejection("acme", "retained_bytes")
	registry.RecordTenantLimitRejection("acme", "connections")
	registry.SetTenantQuotaProfile(true, 3, false)
	registry.SetSessionsActive(true, 1)
	registry.ObserveSubscriptions(subscriptionCount(2))
	registry.SetRetainedStats(1, 16)
	registry.RecordPublishBackpressureRejection("commit_lag")
	registry.SetPublishBackpressureActive("disk_free", true)
	registry.SetStorageFreeBytes(1024)
	registry.SetCommitBacklogOldest(time.Second)
	registry.AddDurableDeliverySaturatedTargets(1)
	registry.SetPeerReplicationLag(map[uint64]uint64{2: 3})
	registry.ObservePublishDuration(2 * time.Millisecond)
	registry.ObserveWALSync(time.Millisecond, 2, 64)
	registry.ObserveRaftStage(RaftApply, time.Millisecond)
	return registry
}

type expositionSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

type expositionFamily struct {
	Type    string
	Help    string
	Samples []expositionSample
}

func parseExposition(t *testing.T, text string) map[string]*expositionFamily {
	t.Helper()
	families := make(map[string]*expositionFamily)
	scanner := bufio.NewScanner(strings.NewReader(text))
	currentFamily := ""
	lastHelpName := ""
	lastHelpValue := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "# EOF" {
			lastHelpName = ""
			lastHelpValue = ""
			continue
		}
		if strings.HasPrefix(line, "# HELP ") {
			parts := strings.SplitN(strings.TrimPrefix(line, "# HELP "), " ", 2)
			if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
				t.Errorf("malformed or empty HELP line %q", line)
				lastHelpName = ""
				lastHelpValue = ""
				continue
			}
			lastHelpName = parts[0]
			lastHelpValue = parts[1]
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			parts := strings.Fields(strings.TrimPrefix(line, "# TYPE "))
			if len(parts) != 2 {
				t.Errorf("malformed TYPE line %q", line)
				lastHelpName = ""
				lastHelpValue = ""
				continue
			}
			name, metricType := parts[0], parts[1]
			if lastHelpName != name {
				t.Errorf("%s: TYPE must immediately follow its HELP line", name)
			}
			if _, exists := families[name]; exists {
				t.Errorf("%s: duplicate TYPE family", name)
				lastHelpName = ""
				lastHelpValue = ""
				continue
			}
			families[name] = &expositionFamily{Type: metricType, Help: lastHelpValue}
			currentFamily = name
			lastHelpName = ""
			lastHelpValue = ""
			continue
		}
		if strings.HasPrefix(line, "#") {
			t.Errorf("unexpected exposition comment %q", line)
			lastHelpName = ""
			lastHelpValue = ""
			continue
		}
		lastHelpName = ""
		lastHelpValue = ""

		parts := expositionSamplePattern.FindStringSubmatch(line)
		if parts == nil {
			t.Errorf("malformed sample line %q", line)
			continue
		}
		if currentFamily == "" {
			t.Errorf("sample %q has no preceding TYPE family", line)
			continue
		}
		labels, err := parseExpositionLabels(parts[2])
		if err != nil {
			t.Errorf("parse labels in %q: %v", line, err)
			continue
		}
		value, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			t.Errorf("parse value in %q: %v", line, err)
			continue
		}

		family := families[currentFamily]
		sampleName := parts[1]
		switch family.Type {
		case "counter", "gauge":
			if sampleName != currentFamily {
				t.Errorf("%s: %s sample name must equal its family name", currentFamily, family.Type)
			}
		case "histogram":
			if sampleName != currentFamily+"_bucket" && sampleName != currentFamily+"_sum" && sampleName != currentFamily+"_count" {
				t.Errorf("%s: invalid histogram sample name %q", currentFamily, sampleName)
			}
		}
		family.Samples = append(family.Samples, expositionSample{Name: sampleName, Labels: labels, Value: value})
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read exposition: %v", err)
	}
	return families
}

func parseExpositionLabels(raw string) (map[string]string, error) {
	labels := make(map[string]string)
	for index := 0; index < len(raw); {
		keyStart := index
		for index < len(raw) && raw[index] != '=' {
			index++
		}
		if index == len(raw) {
			return nil, fmt.Errorf("label key has no equals sign")
		}
		key := raw[keyStart:index]
		if key == "" {
			return nil, fmt.Errorf("label key is empty")
		}
		index++
		if index == len(raw) || raw[index] != '"' {
			return nil, fmt.Errorf("label %q value is not quoted", key)
		}
		index++
		var value strings.Builder
		closed := false
		for index < len(raw) {
			character := raw[index]
			index++
			if character == '"' {
				closed = true
				break
			}
			if character != '\\' {
				value.WriteByte(character)
				continue
			}
			if index == len(raw) {
				return nil, fmt.Errorf("label %q ends with an escape", key)
			}
			escape := raw[index]
			index++
			switch escape {
			case '\\':
				value.WriteByte('\\')
			case '"':
				value.WriteByte('"')
			case 'n':
				value.WriteByte('\n')
			default:
				return nil, fmt.Errorf("label %q has unsupported escape \\%c", key, escape)
			}
		}
		if !closed {
			return nil, fmt.Errorf("label %q has an unterminated value", key)
		}
		if _, exists := labels[key]; exists {
			return nil, fmt.Errorf("duplicate label %q", key)
		}
		labels[key] = value.String()
		if index == len(raw) {
			break
		}
		if raw[index] != ',' {
			return nil, fmt.Errorf("expected comma after label %q", key)
		}
		index++
		if index == len(raw) {
			return nil, fmt.Errorf("label list ends after comma")
		}
	}
	return labels, nil
}

// Traceability: API-03; docs/specs/management-api-and-observability.md §7.1.
// Scenarios: management-api-and-observability.md:MGT-SCN-027
func TestRegistryFamiliesMatchAggregationCatalog(t *testing.T) {
	catalog := loadAggregationCatalog(t)
	for _, format := range []struct {
		name        string
		openMetrics bool
	}{
		{name: "prometheus"},
		{name: "openmetrics", openMetrics: true},
	} {
		t.Run(format.name, func(t *testing.T) {
			families := parseExposition(t, string(fullyObservedRegistry().Render(format.openMetrics)))
			catalogNames := sortedCatalogNames(catalog)
			familyNames := sortedFamilyNames(families)
			if !equalStrings(familyNames, catalogNames) {
				var missing, extra []string
				for _, name := range catalogNames {
					if _, exists := families[name]; !exists {
						missing = append(missing, name)
					}
				}
				for _, name := range familyNames {
					if _, exists := catalog[name]; !exists {
						extra = append(extra, name)
					}
				}
				t.Errorf("TYPE family mismatch: missing=%v extra=%v", missing, extra)
			}

			for _, name := range catalogNames {
				entry := catalog[name]
				family, exists := families[name]
				if !exists {
					continue
				}
				if family.Help != entry.Help {
					t.Errorf("%s: HELP = %q, catalog help = %q", name, family.Help, entry.Help)
				}
				if family.Type != string(entry.Type) {
					t.Errorf("%s: TYPE = %q, catalog type = %q", name, family.Type, entry.Type)
				}
				if len(family.Samples) == 0 {
					t.Errorf("%s: family has no samples", name)
				}
				for _, sample := range family.Samples {
					expectedLabels := append([]string(nil), entry.LabelNames...)
					if entry.Type == ports.MetricTypeHistogram && sample.Name == name+"_bucket" {
						expectedLabels = append(expectedLabels, "le")
					}
					actualLabels := sortedLabelNames(sample.Labels)
					sort.Strings(expectedLabels)
					if !equalStrings(actualLabels, expectedLabels) {
						t.Errorf("%s sample %q label names = %v, catalog requires %v", name, sample.Name, actualLabels, expectedLabels)
					}
					if sample.Value < 0 || math.IsNaN(sample.Value) {
						t.Errorf("%s sample %q has invalid value %v", name, sample.Name, sample.Value)
					}
				}
				if entry.Type == ports.MetricTypeHistogram {
					validateHistogramBuckets(t, name, family)
				}
			}
		})
	}
}

func validateHistogramBuckets(t *testing.T, familyName string, family *expositionFamily) {
	t.Helper()
	type bucket struct {
		le    string
		bound float64
		value float64
	}
	bucketsByLabels := make(map[string][]bucket)
	countsByLabels := make(map[string]float64)
	for _, sample := range family.Samples {
		switch sample.Name {
		case familyName + "_bucket":
			le, exists := sample.Labels["le"]
			if !exists {
				t.Errorf("%s: bucket sample has no le label", familyName)
				continue
			}
			bound, err := strconv.ParseFloat(le, 64)
			if err != nil || math.IsNaN(bound) {
				t.Errorf("%s: invalid bucket boundary %q", familyName, le)
				continue
			}
			bucketsByLabels[labelSignature(sample.Labels, "le")] = append(bucketsByLabels[labelSignature(sample.Labels, "le")], bucket{
				le: le, bound: bound, value: sample.Value,
			})
		case familyName + "_count":
			signature := labelSignature(sample.Labels, "")
			if _, exists := countsByLabels[signature]; exists {
				t.Errorf("%s: duplicate _count sample for labels %v", familyName, sample.Labels)
			}
			countsByLabels[signature] = sample.Value
		}
	}
	if len(bucketsByLabels) == 0 {
		t.Errorf("%s: histogram has no _bucket samples", familyName)
	}
	for signature := range countsByLabels {
		if _, exists := bucketsByLabels[signature]; !exists {
			t.Errorf("%s: _count sample for labels %q has no _bucket series", familyName, signature)
		}
	}

	var commonBoundaries []string
	commonBoundariesSet := false
	for signature, buckets := range bucketsByLabels {
		boundaries := make([]string, 0, len(buckets))
		previous := math.Inf(-1)
		for _, current := range buckets {
			if current.bound <= previous {
				t.Errorf("%s: bucket boundaries are not strictly increasing for labels %q", familyName, signature)
			}
			if len(boundaries) > 0 && current.value < buckets[len(boundaries)-1].value {
				t.Errorf("%s: cumulative bucket count decreased for labels %q", familyName, signature)
			}
			boundaries = append(boundaries, current.le)
			previous = current.bound
		}
		if len(buckets) == 0 || buckets[len(buckets)-1].le != "+Inf" {
			t.Errorf("%s: bucket series for labels %q must end with +Inf", familyName, signature)
		} else if count, exists := countsByLabels[signature]; !exists || buckets[len(buckets)-1].value != count {
			t.Errorf("%s: +Inf bucket count %v does not match _count %v for labels %q", familyName, buckets[len(buckets)-1].value, count, signature)
		}
		if !commonBoundariesSet {
			commonBoundaries = boundaries
			commonBoundariesSet = true
		} else if !equalStrings(boundaries, commonBoundaries) {
			t.Errorf("%s: bucket boundaries differ for labels %q: got %v, want %v", familyName, signature, boundaries, commonBoundaries)
		}
	}
}

func labelSignature(labels map[string]string, ignored string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if key != ignored {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var signature strings.Builder
	for _, key := range keys {
		value := labels[key]
		fmt.Fprintf(&signature, "%d:%s%d:%s", len(key), key, len(value), value)
	}
	return signature.String()
}

func sortedLabelNames(labels map[string]string) []string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedCatalogNames(catalog map[string]aggregationCatalogEntry) []string {
	names := make([]string, 0, len(catalog))
	for name := range catalog {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedFamilyNames(families map[string]*expositionFamily) []string {
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
