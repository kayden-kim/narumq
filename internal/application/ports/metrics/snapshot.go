package metrics

// MetricValueKind identifies the numeric representation used by a metric sample.
type MetricValueKind string

const (
	// MetricValueInteger identifies an exact unsigned integer metric value.
	MetricValueInteger MetricValueKind = "integer"
	// MetricValueFloat identifies a floating-point metric value.
	MetricValueFloat MetricValueKind = "float"
)

// MetricType identifies the Prometheus metric family type.
type MetricType string

const (
	// MetricTypeCounter identifies a monotonically increasing counter family.
	MetricTypeCounter MetricType = "counter"
	// MetricTypeGauge identifies a gauge family.
	MetricTypeGauge MetricType = "gauge"
	// MetricTypeHistogram identifies a histogram family.
	MetricTypeHistogram MetricType = "histogram"
)

// MetricScope identifies which node or nodes a metric describes.
type MetricScope string

const (
	// MetricScopeNode identifies a value observed or processed by this node.
	MetricScopeNode MetricScope = "node"
	// MetricScopeReplicated identifies a value derived from replicated committed state.
	MetricScopeReplicated MetricScope = "replicated"
	// MetricScopeLeader identifies a value emitted only by the Raft leader.
	MetricScopeLeader MetricScope = "leader"
)

// MetricAggregation identifies the stateless cluster aggregation rule for a family.
type MetricAggregation string

const (
	// MetricAggregationIncreaseSum sums per-node counter increases, not raw counter values.
	MetricAggregationIncreaseSum MetricAggregation = "increase_sum"
	// MetricAggregationSum sums gauge values across nodes.
	MetricAggregationSum MetricAggregation = "sum"
	// MetricAggregationMax selects the maximum gauge value across nodes.
	MetricAggregationMax MetricAggregation = "max"
	// MetricAggregationPerNode preserves the node identity of each gauge value.
	MetricAggregationPerNode MetricAggregation = "per_node"
	// MetricAggregationBucketSum combines histogram bucket observations across nodes.
	MetricAggregationBucketSum MetricAggregation = "bucket_sum"
)

// MetricLabelDTO is one name-value label pair attached to a metric sample.
type MetricLabelDTO struct {
	Name  string
	Value string
}

// MetricValueDTO stores a metric value without converting integers to floating point.
type MetricValueDTO struct {
	Kind    MetricValueKind
	Uint64  uint64
	Float64 float64
}

// MetricDescriptorDTO describes a metric family and its aggregation metadata.
type MetricDescriptorDTO struct {
	Name        string
	Help        string
	Type        MetricType
	Scope       MetricScope
	Aggregation MetricAggregation
	LabelNames  []string
}

// MetricSampleDTO is one named metric sample with its labels and typed value.
type MetricSampleDTO struct {
	Name   string
	Labels []MetricLabelDTO
	Value  MetricValueDTO
}

// MetricFamilyDTO groups a descriptor with the samples captured for that family.
type MetricFamilyDTO struct {
	Descriptor MetricDescriptorDTO
	Samples    []MetricSampleDTO
}

// MetricsSnapshotDTO contains the metric families captured from one registry.
type MetricsSnapshotDTO struct {
	Families []MetricFamilyDTO
}

// MetricVisitor receives a sequence of metric families and their samples.
type MetricVisitor interface {
	BeginFamily(MetricDescriptorDTO) error
	Sample(MetricSampleDTO) error
	EndFamily() error
}

// MetricsSnapshotSource captures a typed snapshot of the registry metrics.
type MetricsSnapshotSource interface {
	CaptureMetrics() (MetricsSnapshotDTO, error)
}
