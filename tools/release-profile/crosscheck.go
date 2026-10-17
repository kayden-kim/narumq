package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v3"
)

// The descriptor pins values that the committed EKS manifests must also carry.
// CrossCheckManifests parses the manifests structurally and reports every mismatch.

type statefulSetManifest struct {
	Spec struct {
		Replicas       int `yaml:"replicas"`
		UpdateStrategy struct {
			Type string `yaml:"type"`
		} `yaml:"updateStrategy"`
		Template struct {
			Spec struct {
				TerminationGracePeriodSeconds int `yaml:"terminationGracePeriodSeconds"`
				Affinity                      struct {
					PodAntiAffinity struct {
						Required []struct {
							TopologyKey string `yaml:"topologyKey"`
						} `yaml:"requiredDuringSchedulingIgnoredDuringExecution"`
					} `yaml:"podAntiAffinity"`
				} `yaml:"affinity"`
				TopologySpreadConstraints []struct {
					MaxSkew           int    `yaml:"maxSkew"`
					TopologyKey       string `yaml:"topologyKey"`
					WhenUnsatisfiable string `yaml:"whenUnsatisfiable"`
				} `yaml:"topologySpreadConstraints"`
				Containers []struct {
					Name      string `yaml:"name"`
					Resources struct {
						Requests map[string]string `yaml:"requests"`
						Limits   map[string]string `yaml:"limits"`
					} `yaml:"resources"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
		VolumeClaimTemplates []struct {
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				AccessModes      []string `yaml:"accessModes"`
				StorageClassName string   `yaml:"storageClassName"`
				Resources        struct {
					Requests map[string]string `yaml:"requests"`
				} `yaml:"resources"`
			} `yaml:"spec"`
		} `yaml:"volumeClaimTemplates"`
	} `yaml:"spec"`
}

type pdbManifest struct {
	Spec struct {
		MaxUnavailable int `yaml:"maxUnavailable"`
	} `yaml:"spec"`
}

type serviceManifest struct {
	Metadata struct {
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
	Spec struct {
		Type  string `yaml:"type"`
		Ports []struct {
			Name     string `yaml:"name"`
			Port     int    `yaml:"port"`
			Protocol string `yaml:"protocol"`
		} `yaml:"ports"`
	} `yaml:"spec"`
}

const (
	brokerContainerName = "narumq"
	brokerDataClaimName = "data"
	nlbMQTTPort         = 1883
)

func decodeManifest(dir, name string, out any) error {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}

// CrossCheckManifests compares the reference profile's pins with the committed
// deploy/kubernetes/eks-ec2-ha manifests (statefulset.yaml, pdb.yaml, service-nlb.yaml).
func CrossCheckManifests(d Descriptor, dir string) error {
	profile, ok := d.Profile(referenceProfileID)
	if !ok || profile.Qualification == nil {
		return fmt.Errorf("descriptor has no %s qualification to cross-check", referenceProfileID)
	}
	q := *profile.Qualification

	var statefulSet statefulSetManifest
	var pdb pdbManifest
	var service serviceManifest
	for _, manifest := range []struct {
		name   string
		target any
	}{{"statefulset.yaml", &statefulSet}, {"pdb.yaml", &pdb}, {"service-nlb.yaml", &service}} {
		if err := decodeManifest(dir, manifest.name, manifest.target); err != nil {
			return err
		}
	}

	var mismatches []string
	compare := func(what string, want, got any) {
		if want != got {
			mismatches = append(mismatches, fmt.Sprintf("%s: descriptor %v, manifest %v", what, want, got))
		}
	}

	spec := statefulSet.Spec
	compare("statefulset replicas", q.Broker.Replicas, spec.Replicas)
	compare("statefulset updateStrategy.type", q.Rollout.UpdateStrategy, spec.UpdateStrategy.Type)
	compare("statefulset terminationGracePeriodSeconds", q.Rollout.TerminationGracePeriodSeconds, spec.Template.Spec.TerminationGracePeriodSeconds)
	compare("pdb maxUnavailable", q.Rollout.PodDisruptionBudgetMaxUnavailable, pdb.Spec.MaxUnavailable)

	antiAffinityKeys := make([]string, 0, len(spec.Template.Spec.Affinity.PodAntiAffinity.Required))
	for _, term := range spec.Template.Spec.Affinity.PodAntiAffinity.Required {
		antiAffinityKeys = append(antiAffinityKeys, term.TopologyKey)
	}
	compare("required anti-affinity topologyKey", q.Topology.AntiAffinityTopologyKey, strings.Join(antiAffinityKeys, ","))

	spreadMatches := 0
	for _, spread := range spec.Template.Spec.TopologySpreadConstraints {
		if spread.TopologyKey != q.Topology.SpreadTopologyKey {
			continue
		}
		spreadMatches++
		compare("topology spread maxSkew", q.Topology.SpreadMaxSkew, spread.MaxSkew)
		compare("topology spread whenUnsatisfiable", "DoNotSchedule", spread.WhenUnsatisfiable)
	}
	compare("topology spread constraints on "+q.Topology.SpreadTopologyKey, 1, spreadMatches)

	foundBroker := false
	for _, container := range spec.Template.Spec.Containers {
		if container.Name != brokerContainerName {
			continue
		}
		foundBroker = true
		compare("broker cpu request", q.Broker.CPURequestsAndLimits, container.Resources.Requests["cpu"])
		compare("broker cpu limit", q.Broker.CPURequestsAndLimits, container.Resources.Limits["cpu"])
		compare("broker memory request", q.Broker.MemoryRequestsAndLimits, container.Resources.Requests["memory"])
		compare("broker memory limit", q.Broker.MemoryRequestsAndLimits, container.Resources.Limits["memory"])
	}
	compare("broker container "+brokerContainerName+" present", true, foundBroker)

	foundClaim := false
	for _, claim := range spec.VolumeClaimTemplates {
		if claim.Metadata.Name != brokerDataClaimName {
			continue
		}
		foundClaim = true
		compare("volumeClaimTemplate storageClassName", q.Storage.StorageClass, claim.Spec.StorageClassName)
		compare("volumeClaimTemplate storage request", fmt.Sprintf("%dGi", q.Storage.SizeGiB), claim.Spec.Resources.Requests["storage"])
		compare("volumeClaimTemplate accessModes", q.Storage.AccessMode, strings.Join(claim.Spec.AccessModes, ","))
	}
	compare("volumeClaimTemplate "+brokerDataClaimName+" present (per-Pod volume)", q.Storage.PerPodVolume, foundClaim)

	annotations := service.Metadata.Annotations
	compare("nlb service type", "LoadBalancer", service.Spec.Type)
	compare("nlb annotation aws-load-balancer-type", "external", annotations["service.beta.kubernetes.io/aws-load-balancer-type"])
	compare("nlb annotation target-type", q.LoadBalancer.TargetType, annotations["service.beta.kubernetes.io/aws-load-balancer-nlb-target-type"])
	compare("nlb annotation scheme", q.LoadBalancer.Scheme, annotations["service.beta.kubernetes.io/aws-load-balancer-scheme"])
	listeners := 0
	for _, port := range service.Spec.Ports {
		listeners++
		compare("nlb listener port", nlbMQTTPort, port.Port)
		compare("nlb listener protocol", q.LoadBalancer.ListenerProtocol, port.Protocol)
	}
	compare("nlb listener count", 1, listeners)

	if len(mismatches) > 0 {
		return fmt.Errorf("descriptor disagrees with %s: %s", filepath.ToSlash(dir), strings.Join(mismatches, "; "))
	}
	return nil
}
