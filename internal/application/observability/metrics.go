package observability

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	metricsports "narumq/internal/application/ports/metrics"
	observationports "narumq/internal/application/ports/observability"
)

const (
	maxMetricFamilies = 64
	maxMetricSamples  = 16384
	maxMetricLabels   = 8
	maxLabelValueSize = 1024
)

func metricFamiliesFromSnapshot(snapshot metricsports.MetricsSnapshotDTO) ([]observationports.MetricFamilyDTO, error) {
	if len(snapshot.Families) > maxMetricFamilies {
		return nil, errors.New("metric family limit exceeded")
	}
	catalog := make(map[string]metricsports.MetricDescriptorDTO)
	for _, descriptor := range metricsports.MetricCatalog() {
		catalog[descriptor.Name] = descriptor
	}
	families := make([]observationports.MetricFamilyDTO, 0, len(snapshot.Families))
	seenFamilies := make(map[string]struct{}, len(snapshot.Families))
	totalSamples := 0
	for _, family := range snapshot.Families {
		descriptor := family.Descriptor
		canonical, exists := catalog[descriptor.Name]
		if !exists || !sameDescriptor(canonical, descriptor) {
			return nil, errors.New("metric descriptor does not match catalog")
		}
		if _, exists := seenFamilies[descriptor.Name]; exists {
			return nil, errors.New("duplicate metric family")
		}
		seenFamilies[descriptor.Name] = struct{}{}
		totalSamples += len(family.Samples)
		if totalSamples > maxMetricSamples {
			return nil, errors.New("metric sample limit exceeded")
		}
		output := observationports.MetricFamilyDTO{
			Name: descriptor.Name, Help: descriptor.Help, Type: string(descriptor.Type),
			Scope: string(descriptor.Scope), Aggregation: string(descriptor.Aggregation),
			LabelNames: append([]string(nil), descriptor.LabelNames...),
			Samples:    make([]observationports.MetricSampleDTO, 0, len(family.Samples)),
		}
		seenSamples := make(map[string]struct{}, len(family.Samples))
		for _, sample := range family.Samples {
			labels, err := validateMetricSample(descriptor, sample)
			if err != nil {
				return nil, err
			}
			identity := sampleIdentity(sample.Name, labels)
			if _, exists := seenSamples[identity]; exists {
				return nil, errors.New("duplicate metric sample")
			}
			seenSamples[identity] = struct{}{}
			valueKind, value, err := metricValueString(descriptor, sample)
			if err != nil {
				return nil, err
			}
			outputLabels := make([]observationports.MetricLabelDTO, 0, len(labels))
			for name, value := range labels {
				outputLabels = append(outputLabels, observationports.MetricLabelDTO{Name: name, Value: value})
			}
			sort.Slice(outputLabels, func(i, j int) bool { return outputLabels[i].Name < outputLabels[j].Name })
			output.Samples = append(output.Samples, observationports.MetricSampleDTO{
				Name: sample.Name, Labels: outputLabels, ValueKind: valueKind, Value: value,
			})
		}
		if descriptor.Type == metricsports.MetricTypeHistogram {
			if err := validateHistogram(descriptor, family.Samples); err != nil {
				return nil, err
			}
		}
		sortMetricSamples(output.Samples)
		families = append(families, output)
	}
	sort.Slice(families, func(i, j int) bool { return families[i].Name < families[j].Name })
	return families, nil
}

func sameDescriptor(left, right metricsports.MetricDescriptorDTO) bool {
	if left.Name != right.Name || left.Help != right.Help || left.Type != right.Type ||
		left.Scope != right.Scope || left.Aggregation != right.Aggregation || len(left.LabelNames) != len(right.LabelNames) {
		return false
	}
	for index := range left.LabelNames {
		if left.LabelNames[index] != right.LabelNames[index] {
			return false
		}
	}
	return true
}

func validateMetricSample(descriptor metricsports.MetricDescriptorDTO, sample metricsports.MetricSampleDTO) (map[string]string, error) {
	if len(sample.Labels) > maxMetricLabels {
		return nil, errors.New("metric label limit exceeded")
	}
	expectedName := descriptor.Name
	expectedLabels := descriptor.LabelNames
	switch descriptor.Type {
	case metricsports.MetricTypeCounter, metricsports.MetricTypeGauge:
		if sample.Name != expectedName {
			return nil, errors.New("metric sample name does not match family")
		}
	case metricsports.MetricTypeHistogram:
		switch sample.Name {
		case descriptor.Name + "_bucket":
			expectedLabels = appendExpectedLabel(descriptor.LabelNames, "le")
		case descriptor.Name + "_sum", descriptor.Name + "_count":
		default:
			return nil, errors.New("invalid histogram sample name")
		}
	default:
		return nil, errors.New("invalid metric type")
	}
	labels := make(map[string]string, len(sample.Labels))
	for _, label := range sample.Labels {
		if _, exists := labels[label.Name]; exists {
			return nil, errors.New("duplicate metric label name")
		}
		if !utf8.ValidString(label.Name) || !utf8.ValidString(label.Value) || len(label.Value) > maxLabelValueSize {
			return nil, errors.New("invalid metric label value")
		}
		labels[label.Name] = label.Value
	}
	if len(labels) != len(expectedLabels) {
		return nil, errors.New("metric labels do not match descriptor")
	}
	for _, name := range expectedLabels {
		if _, exists := labels[name]; !exists {
			return nil, errors.New("metric labels do not match descriptor")
		}
	}
	return labels, nil
}

func appendExpectedLabel(labels []string, name string) []string {
	result := make([]string, 0, len(labels)+1)
	result = append(result, labels...)
	return append(result, name)
}

func metricValueString(descriptor metricsports.MetricDescriptorDTO, sample metricsports.MetricSampleDTO) (string, string, error) {
	expectedKind := metricsports.MetricValueFloat
	switch descriptor.Type {
	case metricsports.MetricTypeCounter:
		expectedKind = metricsports.MetricValueInteger
	case metricsports.MetricTypeHistogram:
		if strings.HasSuffix(sample.Name, "_sum") {
			expectedKind = metricsports.MetricValueFloat
		} else {
			expectedKind = metricsports.MetricValueInteger
		}
	case metricsports.MetricTypeGauge:
		if sample.Value.Kind != metricsports.MetricValueInteger && sample.Value.Kind != metricsports.MetricValueFloat {
			return "", "", errors.New("invalid gauge value kind")
		}
		expectedKind = sample.Value.Kind
	}
	if sample.Value.Kind != expectedKind {
		return "", "", errors.New("metric value kind does not match family")
	}
	if expectedKind == metricsports.MetricValueInteger {
		if sample.Value.Float64 != 0 {
			return "", "", errors.New("invalid integer metric representation")
		}
		return string(metricsports.MetricValueInteger), strconv.FormatUint(sample.Value.Uint64, 10), nil
	}
	if sample.Value.Uint64 != 0 || math.IsNaN(sample.Value.Float64) || math.IsInf(sample.Value.Float64, 0) {
		return "", "", errors.New("invalid floating-point metric representation")
	}
	return string(metricsports.MetricValueFloat), strconv.FormatFloat(sample.Value.Float64, 'g', -1, 64), nil
}

type histogramBucket struct {
	boundary float64
	value    uint64
}

type histogramGroup struct {
	buckets []histogramBucket
	count   *uint64
	sumSeen bool
}

func validateHistogram(descriptor metricsports.MetricDescriptorDTO, samples []metricsports.MetricSampleDTO) error {
	groups := make(map[string]*histogramGroup)
	for _, sample := range samples {
		labels := make(map[string]string, len(sample.Labels))
		for _, label := range sample.Labels {
			labels[label.Name] = label.Value
		}
		key := sampleIdentity("", labelsWithout(labels, "le"))
		group := groups[key]
		if group == nil {
			group = &histogramGroup{}
			groups[key] = group
		}
		switch sample.Name {
		case descriptor.Name + "_bucket":
			text := labels["le"]
			if text == "+Inf" {
				group.buckets = append(group.buckets, histogramBucket{boundary: math.Inf(1), value: sample.Value.Uint64})
				continue
			}
			boundary, err := strconv.ParseFloat(text, 64)
			if err != nil || math.IsNaN(boundary) || math.IsInf(boundary, 0) {
				return errors.New("invalid histogram bucket boundary")
			}
			group.buckets = append(group.buckets, histogramBucket{boundary: boundary, value: sample.Value.Uint64})
		case descriptor.Name + "_count":
			if group.count != nil {
				return errors.New("duplicate histogram count")
			}
			count := sample.Value.Uint64
			group.count = &count
		case descriptor.Name + "_sum":
			if group.sumSeen {
				return errors.New("duplicate histogram sum")
			}
			group.sumSeen = true
		}
	}
	var commonBoundaries []float64
	for _, group := range groups {
		if group.count == nil || !group.sumSeen || len(group.buckets) == 0 {
			return errors.New("incomplete histogram series")
		}
		sort.Slice(group.buckets, func(i, j int) bool { return group.buckets[i].boundary < group.buckets[j].boundary })
		boundaries := make([]float64, 0, len(group.buckets))
		previous := math.Inf(-1)
		for _, bucket := range group.buckets {
			if bucket.boundary <= previous {
				return errors.New("histogram bucket boundaries are not strictly increasing")
			}
			if len(boundaries) > 0 && bucket.value < group.buckets[len(boundaries)-1].value {
				return errors.New("histogram bucket counts decreased")
			}
			boundaries = append(boundaries, bucket.boundary)
			previous = bucket.boundary
		}
		if !math.IsInf(group.buckets[len(group.buckets)-1].boundary, 1) || group.buckets[len(group.buckets)-1].value != *group.count {
			return errors.New("histogram +Inf bucket does not match count")
		}
		if commonBoundaries == nil {
			commonBoundaries = boundaries
		} else if !sameFloatBoundaries(commonBoundaries, boundaries) {
			return errors.New("histogram bucket schemas differ")
		}
	}
	return nil
}

func sameFloatBoundaries(left, right []float64) bool {
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

func labelsWithout(labels map[string]string, excluded string) map[string]string {
	result := make(map[string]string, len(labels))
	for name, value := range labels {
		if name != excluded {
			result[name] = value
		}
	}
	return result
}

func combineMetricFamilies(results []observationports.NodeMetricsDataDTO, mode observationports.MetricsMode) ([]observationports.MetricFamilyDTO, error) {
	if mode == observationports.MetricsModeNode {
		if len(results) != 1 || results[0].QueryStatus != observationports.QueryStatusOK {
			return nil, errors.New("invalid local metrics result")
		}
		return cloneMetricFamilies(results[0].Families), nil
	}
	families := make(map[string]*observationports.MetricFamilyDTO)
	for _, result := range results {
		if result.QueryStatus != observationports.QueryStatusOK {
			continue
		}
		if result.NodeID == "" {
			return nil, errors.New("metric result has no node ID")
		}
		for _, family := range result.Families {
			current := families[family.Name]
			if current == nil {
				copy := observationports.MetricFamilyDTO{
					Name: family.Name, Help: family.Help, Type: family.Type, Scope: family.Scope,
					Aggregation: family.Aggregation, LabelNames: append([]string(nil), family.LabelNames...),
				}
				families[family.Name] = &copy
				current = &copy
			} else if !sameMetricFamilyDescriptor(*current, family) {
				return nil, errors.New("metric descriptors differ across nodes")
			}
			addNodeID := mode == observationports.MetricsModeAllNodesUnaggregated ||
				family.Type != string(metricsports.MetricTypeGauge) || family.Aggregation == string(metricsports.MetricAggregationPerNode)
			if addNodeID {
				if !containsString(current.LabelNames, "node_id") {
					current.LabelNames = append(current.LabelNames, "node_id")
				}
			}
			for _, sample := range family.Samples {
				copy := cloneMetricSample(sample)
				if addNodeID {
					copy.Labels = append(copy.Labels, observationports.MetricLabelDTO{Name: "node_id", Value: result.NodeID})
					sort.Slice(copy.Labels, func(i, j int) bool { return copy.Labels[i].Name < copy.Labels[j].Name })
				}
				if mode == observationports.MetricsModeAllNodesAggregated && family.Type == string(metricsports.MetricTypeGauge) &&
					family.Aggregation != string(metricsports.MetricAggregationPerNode) {
					if err := reduceGauge(current, copy, family.Aggregation); err != nil {
						return nil, err
					}
				} else {
					current.Samples = append(current.Samples, copy)
				}
			}
		}
	}
	output := make([]observationports.MetricFamilyDTO, 0, len(families))
	for _, family := range families {
		sortMetricSamples(family.Samples)
		output = append(output, *family)
	}
	sort.Slice(output, func(i, j int) bool { return output[i].Name < output[j].Name })
	return output, nil
}

func sameMetricFamilyDescriptor(left, right observationports.MetricFamilyDTO) bool {
	leftNames := left.LabelNames
	if containsString(leftNames, "node_id") {
		leftNames = make([]string, 0, len(leftNames)-1)
		for _, name := range left.LabelNames {
			if name != "node_id" {
				leftNames = append(leftNames, name)
			}
		}
	}
	if left.Name != right.Name || left.Help != right.Help || left.Type != right.Type || left.Scope != right.Scope ||
		left.Aggregation != right.Aggregation || len(leftNames) != len(right.LabelNames) {
		return false
	}
	for index := range leftNames {
		if leftNames[index] != right.LabelNames[index] {
			return false
		}
	}
	return true
}

func reduceGauge(family *observationports.MetricFamilyDTO, sample observationports.MetricSampleDTO, aggregation string) error {
	identity := sampleIdentity(sample.Name, metricLabelsToMap(sample.Labels))
	for index := range family.Samples {
		current := &family.Samples[index]
		if sampleIdentity(current.Name, metricLabelsToMap(current.Labels)) != identity {
			continue
		}
		if current.ValueKind != sample.ValueKind {
			return errors.New("gauge value kinds differ across nodes")
		}
		switch aggregation {
		case string(metricsports.MetricAggregationSum):
			if current.ValueKind == string(metricsports.MetricValueInteger) {
				left, err := strconv.ParseUint(current.Value, 10, 64)
				if err != nil {
					return errors.New("invalid integer gauge value")
				}
				right, err := strconv.ParseUint(sample.Value, 10, 64)
				if err != nil {
					return errors.New("invalid integer gauge value")
				}
				total, err := addUint64(left, right)
				if err != nil {
					return err
				}
				current.Value = strconv.FormatUint(total, 10)
				return nil
			}
			left, err := strconv.ParseFloat(current.Value, 64)
			if err != nil {
				return errors.New("invalid floating-point gauge value")
			}
			right, err := strconv.ParseFloat(sample.Value, 64)
			if err != nil || !finiteFloat(left+right) {
				return errors.New("floating-point gauge sum is invalid")
			}
			current.Value = strconv.FormatFloat(left+right, 'g', -1, 64)
			return nil
		case string(metricsports.MetricAggregationMax):
			if current.ValueKind == string(metricsports.MetricValueInteger) {
				left, err := strconv.ParseUint(current.Value, 10, 64)
				if err != nil {
					return errors.New("invalid integer gauge value")
				}
				right, err := strconv.ParseUint(sample.Value, 10, 64)
				if err != nil {
					return errors.New("invalid integer gauge value")
				}
				if right > left {
					current.Value = strconv.FormatUint(right, 10)
				}
				return nil
			}
			left, err := strconv.ParseFloat(current.Value, 64)
			if err != nil {
				return errors.New("invalid floating-point gauge value")
			}
			right, err := strconv.ParseFloat(sample.Value, 64)
			if err != nil {
				return errors.New("invalid floating-point gauge value")
			}
			if right > left {
				current.Value = strconv.FormatFloat(right, 'g', -1, 64)
			}
			return nil
		default:
			return errors.New("unsupported gauge aggregation")
		}
	}
	family.Samples = append(family.Samples, sample)
	return nil
}

func metricLabelsToMap(labels []observationports.MetricLabelDTO) map[string]string {
	result := make(map[string]string, len(labels))
	for _, label := range labels {
		result[label.Name] = label.Value
	}
	return result
}

func sampleIdentity(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var identity strings.Builder
	identity.Grow(len(name) + len(keys)*8)
	identity.WriteString(strconv.Itoa(len(name)))
	identity.WriteByte(':')
	identity.WriteString(name)
	for _, key := range keys {
		value := labels[key]
		identity.WriteString(strconv.Itoa(len(key)))
		identity.WriteByte(':')
		identity.WriteString(key)
		identity.WriteString(strconv.Itoa(len(value)))
		identity.WriteByte(':')
		identity.WriteString(value)
	}
	return identity.String()
}

func cloneMetricFamilies(families []observationports.MetricFamilyDTO) []observationports.MetricFamilyDTO {
	result := make([]observationports.MetricFamilyDTO, len(families))
	for index, family := range families {
		result[index] = family
		result[index].LabelNames = append([]string(nil), family.LabelNames...)
		result[index].Samples = make([]observationports.MetricSampleDTO, len(family.Samples))
		for sampleIndex, sample := range family.Samples {
			result[index].Samples[sampleIndex] = cloneMetricSample(sample)
		}
	}
	return result
}

func cloneMetricSample(sample observationports.MetricSampleDTO) observationports.MetricSampleDTO {
	sample.Labels = append([]observationports.MetricLabelDTO(nil), sample.Labels...)
	return sample
}

func sortMetricSamples(samples []observationports.MetricSampleDTO) {
	sort.Slice(samples, func(i, j int) bool {
		left, right := samples[i], samples[j]
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		for index := 0; index < min(len(left.Labels), len(right.Labels)); index++ {
			if left.Labels[index].Name != right.Labels[index].Name {
				return left.Labels[index].Name < right.Labels[index].Name
			}
			if left.Labels[index].Value != right.Labels[index].Value {
				return left.Labels[index].Value < right.Labels[index].Value
			}
		}
		return len(left.Labels) < len(right.Labels)
	})
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
