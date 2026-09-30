package main

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func loadSmokeProfileForCompare(t *testing.T) Profile {
	t.Helper()
	profile, _, _, err := LoadProfile(filepath.Join("testdata", "smoke.json"))
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestBP03TargetTimeoutIncludesOfflineMilliseconds(t *testing.T) {
	profile := loadSmokeProfileForCompare(t)
	timeout, err := bp03TargetTimeout(profile)
	if err != nil {
		t.Fatal(err)
	}
	const want = 344400 * time.Millisecond
	if timeout != want {
		t.Fatalf("target timeout = %s, want %s", timeout, want)
	}
}

func TestBP03ClusterNodeSpecsUseExactTargetSettings(t *testing.T) {
	fixture := &bp03Fixture{
		NetworkOwnerConfigDirectory: "network-owner-public",
		NodeConfigDirectories:       []string{"node1", "node2", "node3"},
	}
	options := bp03RunOptions{RunID: "bp03-run-test"}

	naru := bp03ClusterNodes("narumq", options, fixture)
	if len(naru) != 3 {
		t.Fatalf("NaruMQ nodes = %d, want 3", len(naru))
	}
	for _, node := range naru {
		if node.ConfigDirectory == fixture.NetworkOwnerConfigDirectory {
			t.Fatalf("NaruMQ broker private config directory aliases network owner public config: %q", node.ConfigDirectory)
		}
	}
	for index, node := range naru {
		name := fmt.Sprintf("node%d", index+1)
		if node.Name != name || node.DNSName != name+".bp03.test" || node.ConfigDirectory != name || !node.MountConfig || node.DataDestination != "/var/lib/narumq" {
			t.Errorf("NaruMQ node %d = %#v", index, node)
		}
		if !reflect.DeepEqual(node.BrokerArgs, []string{"-cluster-config", "/config/node.json"}) {
			t.Errorf("NaruMQ node %s broker args = %#v", name, node.BrokerArgs)
		}
	}

	emqx := bp03ClusterNodes("emqx", options, fixture)
	if len(emqx) != 3 {
		t.Fatalf("EMQX nodes = %d, want 3", len(emqx))
	}
	wantEnvironment := []string{
		"EMQX_NODE__NAME=emqx@node2.bp03.test",
		"EMQX_NODE__COOKIE=bp03-bp03-run-test-emqx-cookie",
		"EMQX_CLUSTER__DISCOVERY_STRATEGY=static",
		"EMQX_CLUSTER__STATIC__SEEDS=[emqx@node1.bp03.test,emqx@node2.bp03.test,emqx@node3.bp03.test]",
		"EMQX_DURABLE_SESSIONS__ENABLE=false",
	}
	for index, node := range emqx {
		name := fmt.Sprintf("node%d", index+1)
		want := append([]string(nil), wantEnvironment...)
		want[0] = "EMQX_NODE__NAME=emqx@" + name + ".bp03.test"
		if !reflect.DeepEqual(node.BrokerEnv, want) {
			t.Errorf("EMQX node %s env = %#v, want %#v", node.Name, node.BrokerEnv, want)
		}
		if node.MountConfig || node.DataDestination != "/opt/emqx/data" || node.LogDestination != "/opt/emqx/log" || node.ReadyLogMarker != "EMQX 5.8.8 is running now!" {
			t.Errorf("EMQX node %s settings = %#v", node.Name, node)
		}
	}
}

func TestBuildBP03ComparisonEquivalenceAndNonEquivalentDimensions(t *testing.T) {
	profile := loadSmokeProfileForCompare(t)
	limits := bp03ResourceLimits{
		BrokerCPUs:       profile.BrokerCPUs,
		BrokerMemory:     profile.BrokerMemoryBytes,
		GeneratorCPUs:    profile.GeneratorCPUs,
		GeneratorMemory:  profile.GeneratorMemoryBytes,
		BrokerReadOnlyFS: true,
		RunnerReadOnlyFS: true,
		NetworkMode:      "bridge",
	}
	manifest := bp03Manifest{
		RunID:         "bp03-test",
		Profile:       bp03ArtifactProfile{RawSHA256: "profile-hash", Resolved: profile},
		RunnerImageID: "sha256:runner",
		Topology:      bp03Topology{Nodes: 3},
		Targets: map[string]bp03TargetManifest{
			"narumq": {ProfileSHA256: "profile-hash", ResourceLimits: limits, RunnerImageID: "sha256:runner", Success: true},
			"emqx":   {ProfileSHA256: "profile-hash", ResourceLimits: limits, RunnerImageID: "sha256:runner", Success: true},
		},
	}
	summary := bp03Summary{Targets: map[string]bp03TargetSummary{
		"narumq": {Valid: true},
		"emqx":   {Valid: true},
	}}
	comparison := buildBP03Comparison(manifest, summary)
	if !comparison.Equivalent || comparison.ReleaseGating || comparison.AckLatencyDirectlyComparable {
		t.Fatalf("comparison equivalence/gating = equivalent:%t releaseGating:%t ackLatencyDirectlyComparable:%t", comparison.Equivalent, comparison.ReleaseGating, comparison.AckLatencyDirectlyComparable)
	}
	want := []bp03Difference{
		{Dimension: "replication", Reason: "EMQX OSS 5.8.8 regular sessions are node-local memory; builtin_raft is Enterprise-only"},
		{Dimension: "durability", Reason: "EMQX PUBACK is not preceded by fsync"},
		{Dimension: "authentication", Reason: "NaruMQ validates JWT at CONNECT; EMQX runs without authenticator"},
	}
	if !reflect.DeepEqual(comparison.NonEquivalent, want) {
		t.Fatalf("nonEquivalent = %#v, want %#v", comparison.NonEquivalent, want)
	}

	cases := []struct {
		name   string
		mutate func(*bp03Manifest)
	}{
		{name: "profile hash", mutate: func(value *bp03Manifest) {
			value.Targets["emqx"] = cloneTargetManifest(value.Targets["emqx"], func(target *bp03TargetManifest) { target.ProfileSHA256 = "different" })
		}},
		{name: "resource limits", mutate: func(value *bp03Manifest) {
			value.Targets["emqx"] = cloneTargetManifest(value.Targets["emqx"], func(target *bp03TargetManifest) { target.ResourceLimits.GeneratorCPUs++ })
		}},
		{name: "runner image", mutate: func(value *bp03Manifest) {
			value.Targets["emqx"] = cloneTargetManifest(value.Targets["emqx"], func(target *bp03TargetManifest) { target.RunnerImageID = "sha256:different" })
		}},
		{name: "topology", mutate: func(value *bp03Manifest) { value.Topology.Nodes = 2 }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneBP03Manifest(manifest)
			test.mutate(&changed)
			if buildBP03Comparison(changed, summary).Equivalent {
				t.Fatal("comparison remained equivalent after a required equivalence input changed")
			}
		})
	}
}

func TestSummarizeBP03RunDoesNotRequireEMQXWALApplicability(t *testing.T) {
	validWorkload := &workloadSummary{Valid: true}
	validResources := &resourceSummary{Valid: true}
	manifest := bp03Manifest{
		RunID: "bp03-summary-test",
		Targets: map[string]bp03TargetManifest{
			"narumq": {
				Valid:           true,
				WorkloadSummary: validWorkload,
				ResourceSummary: validResources,
				WALSummary:      &WALSummary{Valid: true},
			},
			"emqx": {
				Valid:           true,
				WorkloadSummary: validWorkload,
				ResourceSummary: validResources,
				WALSummary:      &WALSummary{Applicability: "not_applicable", Valid: false},
			},
		},
	}
	summary := summarizeBP03Run(manifest)
	if !summary.Valid || summary.ReleaseGating {
		t.Fatalf("summary valid/releaseGating = %t/%t, want true/false", summary.Valid, summary.ReleaseGating)
	}
}

func cloneTargetManifest(value bp03TargetManifest, mutate func(*bp03TargetManifest)) bp03TargetManifest {
	mutate(&value)
	return value
}

func cloneBP03Manifest(value bp03Manifest) bp03Manifest {
	cloned := value
	cloned.Targets = make(map[string]bp03TargetManifest, len(value.Targets))
	for key, target := range value.Targets {
		cloned.Targets[key] = target
	}
	return cloned
}
