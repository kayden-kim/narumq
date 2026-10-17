package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// schemaVersion is the only descriptor schema this tool accepts.
const schemaVersion = "narumq.release.deployment-profiles/v1"

// referenceProfileID is the single initial production profile fixed by PRD DEP-19.
const referenceProfileID = "EKS_EC2_HA"

// Status is the support status a release artifact displays for a deployment profile.
type Status string

const (
	StatusSupported    Status = "Supported"
	StatusExperimental Status = "Experimental"
	StatusUnsupported  Status = "Unsupported"
)

// EvidenceKind names a guarantee that EKS_EC2_HA evidence can support.
type EvidenceKind string

const (
	EvidenceHA            EvidenceKind = "ha"
	EvidenceRPO           EvidenceKind = "rpo"
	EvidenceRollingUpdate EvidenceKind = "rollingUpdate"
)

// requiredProfileIDs are the profiles every descriptor must declare (PRD "지원 배포 프로필").
var requiredProfileIDs = []string{referenceProfileID, "ECS_EC2", "EKS_FARGATE", "COMPOSE_LOCAL"}

// Descriptor is the committed release-artifact deployment-profile descriptor.
type Descriptor struct {
	SchemaVersion    string    `json:"schemaVersion"`
	ReferenceProfile string    `json:"referenceProfile"`
	Profiles         []Profile `json:"profiles"`
}

// Profile is one deployment profile and the evidence it may contribute.
type Profile struct {
	ID                  string         `json:"id"`
	Status              Status         `json:"status"`
	Usage               string         `json:"usage"`
	Scope               string         `json:"scope"`
	EvidenceEligibleFor EvidenceFlags  `json:"evidenceEligibleFor"`
	Qualification       *Qualification `json:"qualification,omitempty"`
}

// EvidenceFlags states, per guarantee, whether the profile's results count as EKS_EC2_HA evidence.
// Pointers make a missing flag a validation error instead of a silent false.
type EvidenceFlags struct {
	HA            *bool `json:"ha"`
	RPO           *bool `json:"rpo"`
	RollingUpdate *bool `json:"rollingUpdate"`
}

// Qualification pins the facts DEP-19 requires the release artifact to identify.
type Qualification struct {
	Kubernetes   KubernetesPin   `json:"kubernetes"`
	NodeGroup    NodeGroupPin    `json:"nodeGroup"`
	Topology     TopologyPin     `json:"topology"`
	Storage      StoragePin      `json:"storage"`
	LoadBalancer LoadBalancerPin `json:"loadBalancer"`
	Broker       BrokerPin       `json:"broker"`
	Rollout      RolloutPin      `json:"rollout"`
}

type KubernetesPin struct {
	Distribution string `json:"distribution"`
	Minor        string `json:"minor"`
}

type NodeGroupPin struct {
	ComputeType  string `json:"computeType"`
	AMIFamily    string `json:"amiFamily"`
	Architecture string `json:"architecture"`
	AMIType      string `json:"amiType"`
	InstanceType string `json:"instanceType"`
	NodeCount    int    `json:"nodeCount"`
}

type TopologyPin struct {
	AvailabilityZoneCount         int    `json:"availabilityZoneCount"`
	BrokerPodsPerAvailabilityZone int    `json:"brokerPodsPerAvailabilityZone"`
	AntiAffinityTopologyKey       string `json:"antiAffinityTopologyKey"`
	SpreadTopologyKey             string `json:"spreadTopologyKey"`
	SpreadMaxSkew                 int    `json:"spreadMaxSkew"`
}

type StoragePin struct {
	Provisioner  string `json:"provisioner"`
	StorageClass string `json:"storageClass"`
	VolumeType   string `json:"volumeType"`
	SizeGiB      int    `json:"sizeGiB"`
	Encrypted    bool   `json:"encrypted"`
	AccessMode   string `json:"accessMode"`
	PerPodVolume bool   `json:"perPodVolume"`
}

type LoadBalancerPin struct {
	Type                 string `json:"type"`
	Scheme               string `json:"scheme"`
	TargetType           string `json:"targetType"`
	ListenerProtocol     string `json:"listenerProtocol"`
	TLSTermination       string `json:"tlsTermination"`
	ControllerMinVersion string `json:"controllerMinVersion"`
}

type BrokerPin struct {
	Replicas                int    `json:"replicas"`
	CPURequestsAndLimits    string `json:"cpuRequestsAndLimits"`
	MemoryRequestsAndLimits string `json:"memoryRequestsAndLimits"`
}

type RolloutPin struct {
	UpdateStrategy                    string `json:"updateStrategy"`
	PodDisruptionBudgetMaxUnavailable int    `json:"podDisruptionBudgetMaxUnavailable"`
	DrainTimeoutSeconds               int    `json:"drainTimeoutSeconds"`
	TerminationGracePeriodSeconds     int    `json:"terminationGracePeriodSeconds"`
}

// terminationBudgetSeconds is the LB wait plus post-drain shutdown budget of spec §7:
// terminationGracePeriodSeconds >= drain seconds + 30 + 30.
const terminationBudgetSeconds = 60

var (
	minorVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)
	semverPattern       = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
)

// DecodeDescriptor strictly decodes and validates descriptor bytes. Unknown fields, trailing
// data and any validation failure are errors.
func DecodeDescriptor(data []byte) (Descriptor, error) {
	var descriptor Descriptor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&descriptor); err != nil {
		return Descriptor{}, fmt.Errorf("decode deployment profile descriptor: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Descriptor{}, errors.New("decode deployment profile descriptor: unexpected data after the JSON document")
	}
	if err := descriptor.Validate(); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

// Validate reports every structural and consistency problem of the descriptor.
func (d Descriptor) Validate() error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if d.SchemaVersion != schemaVersion {
		add("schemaVersion must be %q, got %q", schemaVersion, d.SchemaVersion)
	}
	if d.ReferenceProfile != referenceProfileID {
		add("referenceProfile must be %q, got %q", referenceProfileID, d.ReferenceProfile)
	}

	seen := make(map[string]struct{}, len(d.Profiles))
	for index, profile := range d.Profiles {
		label := fmt.Sprintf("profiles[%d]", index)
		if profile.ID == "" {
			add("%s: id is required", label)
			continue
		}
		label = fmt.Sprintf("profile %s", profile.ID)
		if _, duplicate := seen[profile.ID]; duplicate {
			add("%s: duplicate profile id", label)
		}
		seen[profile.ID] = struct{}{}
		if !isRequiredProfile(profile.ID) {
			add("%s: unknown profile id", label)
		}
		problems = append(problems, validateProfile(label, profile, profile.ID == d.ReferenceProfile)...)
	}
	for _, id := range requiredProfileIDs {
		if _, ok := seen[id]; !ok {
			add("required profile %s is missing", id)
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid deployment profile descriptor: %s", strings.Join(problems, "; "))
	}
	return nil
}

func isRequiredProfile(id string) bool {
	for _, required := range requiredProfileIDs {
		if required == id {
			return true
		}
	}
	return false
}

func validateProfile(label string, profile Profile, reference bool) []string {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, label+": "+fmt.Sprintf(format, args...)) }

	switch profile.Status {
	case StatusSupported, StatusExperimental, StatusUnsupported:
	default:
		add("status must be Supported, Experimental or Unsupported, got %q", profile.Status)
	}
	if strings.TrimSpace(profile.Usage) == "" {
		add("usage is required")
	}
	if strings.TrimSpace(profile.Scope) == "" {
		add("scope is required")
	}
	if reference && profile.Status != StatusSupported {
		add("reference profile must be Supported, got %q", profile.Status)
	}
	if !reference && profile.Status == StatusSupported {
		add("only the reference profile %s may be Supported", referenceProfileID)
	}

	flags := profile.EvidenceEligibleFor
	for _, flag := range []struct {
		name  string
		value *bool
	}{{"ha", flags.HA}, {"rpo", flags.RPO}, {"rollingUpdate", flags.RollingUpdate}} {
		switch {
		case flag.value == nil:
			add("evidenceEligibleFor.%s is required", flag.name)
		case reference && !*flag.value:
			add("evidenceEligibleFor.%s must be true for the reference profile", flag.name)
		case !reference && *flag.value:
			add("evidenceEligibleFor.%s must be false for a non-reference profile", flag.name)
		}
	}

	switch {
	case reference && profile.Qualification == nil:
		add("qualification is required for the reference profile")
	case !reference && profile.Qualification != nil:
		add("qualification is only allowed on the reference profile")
	case profile.Qualification != nil:
		for _, problem := range profile.Qualification.validate() {
			add("qualification.%s", problem)
		}
	}
	return problems
}

func (q Qualification) validate() []string {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	required := func(name, value string) {
		if strings.TrimSpace(value) == "" {
			add("%s is required", name)
		}
	}

	required("kubernetes.distribution", q.Kubernetes.Distribution)
	if !minorVersionPattern.MatchString(q.Kubernetes.Minor) {
		add("kubernetes.minor must look like 1.37, got %q", q.Kubernetes.Minor)
	}

	required("nodeGroup.computeType", q.NodeGroup.ComputeType)
	required("nodeGroup.amiFamily", q.NodeGroup.AMIFamily)
	required("nodeGroup.architecture", q.NodeGroup.Architecture)
	required("nodeGroup.amiType", q.NodeGroup.AMIType)
	required("nodeGroup.instanceType", q.NodeGroup.InstanceType)
	if q.NodeGroup.ComputeType != "EC2" {
		add("nodeGroup.computeType must be EC2, got %q", q.NodeGroup.ComputeType)
	}

	if q.Topology.AvailabilityZoneCount != 3 {
		add("topology.availabilityZoneCount must be 3, got %d", q.Topology.AvailabilityZoneCount)
	}
	if q.Topology.BrokerPodsPerAvailabilityZone != 1 {
		add("topology.brokerPodsPerAvailabilityZone must be 1, got %d", q.Topology.BrokerPodsPerAvailabilityZone)
	}
	required("topology.antiAffinityTopologyKey", q.Topology.AntiAffinityTopologyKey)
	required("topology.spreadTopologyKey", q.Topology.SpreadTopologyKey)
	if q.Topology.SpreadMaxSkew != 1 {
		add("topology.spreadMaxSkew must be 1, got %d", q.Topology.SpreadMaxSkew)
	}
	if q.NodeGroup.NodeCount != q.Topology.AvailabilityZoneCount*q.Topology.BrokerPodsPerAvailabilityZone {
		add("nodeGroup.nodeCount %d must equal availabilityZoneCount x brokerPodsPerAvailabilityZone", q.NodeGroup.NodeCount)
	}
	if q.Broker.Replicas != q.NodeGroup.NodeCount {
		add("broker.replicas %d must equal nodeGroup.nodeCount %d", q.Broker.Replicas, q.NodeGroup.NodeCount)
	}

	required("storage.provisioner", q.Storage.Provisioner)
	required("storage.storageClass", q.Storage.StorageClass)
	required("storage.volumeType", q.Storage.VolumeType)
	required("storage.accessMode", q.Storage.AccessMode)
	if q.Storage.SizeGiB <= 0 {
		add("storage.sizeGiB must be positive, got %d", q.Storage.SizeGiB)
	}
	if !q.Storage.Encrypted {
		add("storage.encrypted must be true")
	}
	if !q.Storage.PerPodVolume {
		add("storage.perPodVolume must be true")
	}

	required("loadBalancer.type", q.LoadBalancer.Type)
	required("loadBalancer.scheme", q.LoadBalancer.Scheme)
	required("loadBalancer.targetType", q.LoadBalancer.TargetType)
	required("loadBalancer.listenerProtocol", q.LoadBalancer.ListenerProtocol)
	required("loadBalancer.tlsTermination", q.LoadBalancer.TLSTermination)
	if !semverPattern.MatchString(q.LoadBalancer.ControllerMinVersion) {
		add("loadBalancer.controllerMinVersion must look like 2.7.2, got %q", q.LoadBalancer.ControllerMinVersion)
	}

	required("broker.cpuRequestsAndLimits", q.Broker.CPURequestsAndLimits)
	required("broker.memoryRequestsAndLimits", q.Broker.MemoryRequestsAndLimits)

	required("rollout.updateStrategy", q.Rollout.UpdateStrategy)
	if q.Rollout.PodDisruptionBudgetMaxUnavailable != 1 {
		add("rollout.podDisruptionBudgetMaxUnavailable must be 1, got %d", q.Rollout.PodDisruptionBudgetMaxUnavailable)
	}
	if q.Rollout.DrainTimeoutSeconds <= 0 {
		add("rollout.drainTimeoutSeconds must be positive, got %d", q.Rollout.DrainTimeoutSeconds)
	}
	if q.Rollout.TerminationGracePeriodSeconds < q.Rollout.DrainTimeoutSeconds+terminationBudgetSeconds {
		add("rollout.terminationGracePeriodSeconds %d must be at least drainTimeoutSeconds %d + %d",
			q.Rollout.TerminationGracePeriodSeconds, q.Rollout.DrainTimeoutSeconds, terminationBudgetSeconds)
	}
	return problems
}

// Profile returns the declared profile with the given id.
func (d Descriptor) Profile(id string) (Profile, bool) {
	for _, profile := range d.Profiles {
		if profile.ID == id {
			return profile, true
		}
	}
	return Profile{}, false
}

// EligibleEvidence reports whether a result produced under profileID may support kind of the
// reference profile. Results from unknown, Experimental or Unsupported profiles are not eligible.
func (d Descriptor) EligibleEvidence(profileID string, kind EvidenceKind) (bool, error) {
	profile, known := d.Profile(profileID)
	var flag *bool
	switch kind {
	case EvidenceHA:
		flag = profile.EvidenceEligibleFor.HA
	case EvidenceRPO:
		flag = profile.EvidenceEligibleFor.RPO
	case EvidenceRollingUpdate:
		flag = profile.EvidenceEligibleFor.RollingUpdate
	default:
		return false, fmt.Errorf("unknown evidence kind %q (want ha, rpo or rollingUpdate)", kind)
	}
	return known && profile.Status == StatusSupported && flag != nil && *flag, nil
}

// Evidence is one verification result tagged with the deployment profile it was produced under.
type Evidence struct {
	Profile string       `json:"profile"`
	Kind    EvidenceKind `json:"kind"`
	Source  string       `json:"source"`
}

// ExcludedEvidence is an evidence record rejected from the reference profile's evidence set.
type ExcludedEvidence struct {
	Evidence Evidence
	Reason   string
}

// FilterEvidence keeps only records eligible for the reference profile and explains each exclusion.
func (d Descriptor) FilterEvidence(records []Evidence) ([]Evidence, []ExcludedEvidence, error) {
	var eligible []Evidence
	var excluded []ExcludedEvidence
	for _, record := range records {
		ok, err := d.EligibleEvidence(record.Profile, record.Kind)
		if err != nil {
			return nil, nil, fmt.Errorf("evidence %q: %w", record.Source, err)
		}
		if ok {
			eligible = append(eligible, record)
			continue
		}
		reason := fmt.Sprintf("profile %q is not declared", record.Profile)
		if profile, known := d.Profile(record.Profile); known {
			reason = fmt.Sprintf("profile %s is %s and cannot support %s evidence of %s", profile.ID, profile.Status, record.Kind, referenceProfileID)
		}
		excluded = append(excluded, ExcludedEvidence{Evidence: record, Reason: reason})
	}
	return eligible, excluded, nil
}

// Display renders the status each profile shows in the release artifact, one line per profile
// in the order the descriptor declares them.
func (d Descriptor) Display() string {
	width := 0
	for _, profile := range d.Profiles {
		if len(profile.ID) > width {
			width = len(profile.ID)
		}
	}
	var out strings.Builder
	for _, profile := range d.Profiles {
		fmt.Fprintf(&out, "%-*s  %-12s  %s\n", width, profile.ID, profile.Status, profile.Usage)
	}
	return out.String()
}
