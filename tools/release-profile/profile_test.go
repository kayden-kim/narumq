package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var (
	realDescriptorPath = filepath.Join("..", "..", "deploy", "release", "deployment-profiles-v1.json")
	realManifestDir    = filepath.Join("..", "..", defaultManifestDir)
)

func realDescriptorBytes(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(realDescriptorPath)
	if err != nil {
		t.Fatalf("read committed descriptor: %v", err)
	}
	return data
}

func realDescriptor(t *testing.T) Descriptor {
	t.Helper()
	descriptor, err := DecodeDescriptor(realDescriptorBytes(t))
	if err != nil {
		t.Fatalf("committed descriptor is invalid: %v", err)
	}
	return descriptor
}

// mutatedDescriptor decodes the committed descriptor into generic JSON, applies mutate and
// re-encodes it, so every negative case differs from the real file in exactly one aspect.
func mutatedDescriptor(t *testing.T, mutate func(root map[string]any)) []byte {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(realDescriptorBytes(t), &root); err != nil {
		t.Fatal(err)
	}
	mutate(root)
	data, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func profileNode(t *testing.T, root map[string]any, id string) map[string]any {
	t.Helper()
	for _, item := range root["profiles"].([]any) {
		node := item.(map[string]any)
		if node["id"] == id {
			return node
		}
	}
	t.Fatalf("profile %s not found in descriptor", id)
	return nil
}

func removeProfile(t *testing.T, root map[string]any, id string) {
	t.Helper()
	var kept []any
	for _, item := range root["profiles"].([]any) {
		if item.(map[string]any)["id"] != id {
			kept = append(kept, item)
		}
	}
	root["profiles"] = kept
}

var allEvidenceKinds = []EvidenceKind{EvidenceHA, EvidenceRPO, EvidenceRollingUpdate}

// Traceability: DEP-19, DEP-23; docs/specs/drain-and-rolling-update.md §7.1, §10.
// Scenarios: drain-and-rolling-update.md:DRN-SCN-032
func TestReleaseArtifactDisplaysProfileStatusAndExcludesUnsupportedEvidence(t *testing.T) {
	descriptor := realDescriptor(t)

	// Expected output 1: Supported, Experimental and Unsupported statuses are displayed.
	wantStatuses := map[string]Status{
		"EKS_EC2_HA":    StatusSupported,
		"ECS_EC2":       StatusExperimental,
		"EKS_FARGATE":   StatusUnsupported,
		"COMPOSE_LOCAL": StatusUnsupported,
	}
	if len(descriptor.Profiles) != len(wantStatuses) {
		t.Fatalf("descriptor declares %d profiles, want %d", len(descriptor.Profiles), len(wantStatuses))
	}
	for id, want := range wantStatuses {
		profile, ok := descriptor.Profile(id)
		if !ok {
			t.Fatalf("profile %s is missing from the release descriptor", id)
		}
		if profile.Status != want {
			t.Errorf("profile %s status = %q, want %q", id, profile.Status, want)
		}
	}
	for _, profile := range descriptor.Profiles {
		if profile.Status == StatusSupported && profile.ID != "EKS_EC2_HA" {
			t.Errorf("profile %s is Supported; only EKS_EC2_HA may be", profile.ID)
		}
	}

	var stdout, stderr bytes.Buffer
	args := []string{"validate", "-descriptor", realDescriptorPath, "-manifests", realManifestDir}
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("validate exit code %d: %s", code, stderr.String())
	}
	displayed := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("unexpected display line %q", line)
		}
		displayed[fields[0]] = fields[1]
	}
	for id, want := range wantStatuses {
		if displayed[id] != string(want) {
			t.Errorf("validate displays %s as %q, want %q (output %q)", id, displayed[id], want, stdout.String())
		}
	}

	// Expected output 2: results of non-Supported profiles never count as EKS_EC2_HA evidence.
	var records []Evidence
	for _, profile := range []string{"EKS_EC2_HA", "ECS_EC2", "EKS_FARGATE", "COMPOSE_LOCAL", "SINGLE_HOST_MULTI_CONTAINER"} {
		for _, kind := range allEvidenceKinds {
			records = append(records, Evidence{Profile: profile, Kind: kind, Source: profile + "/" + string(kind)})
		}
	}
	eligible, excluded, err := descriptor.FilterEvidence(records)
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != len(allEvidenceKinds) {
		t.Fatalf("eligible evidence = %+v, want only the three EKS_EC2_HA records", eligible)
	}
	for _, record := range eligible {
		if record.Profile != "EKS_EC2_HA" {
			t.Errorf("evidence from %s was accepted as EKS_EC2_HA evidence", record.Profile)
		}
	}
	if len(excluded) != len(records)-len(allEvidenceKinds) {
		t.Fatalf("excluded %d records, want %d", len(excluded), len(records)-len(allEvidenceKinds))
	}
	for _, item := range excluded {
		if item.Evidence.Profile == "EKS_EC2_HA" {
			t.Errorf("EKS_EC2_HA evidence %s was excluded", item.Evidence.Source)
		}
		if item.Reason == "" {
			t.Errorf("exclusion of %s has no reason", item.Evidence.Source)
		}
	}
	for _, profile := range []string{"ECS_EC2", "EKS_FARGATE", "COMPOSE_LOCAL", "SINGLE_HOST_MULTI_CONTAINER"} {
		for _, kind := range allEvidenceKinds {
			ok, err := descriptor.EligibleEvidence(profile, kind)
			if err != nil || ok {
				t.Errorf("EligibleEvidence(%s, %s) = %v, %v; want false, nil", profile, kind, ok, err)
			}
		}
	}
	for _, kind := range allEvidenceKinds {
		ok, err := descriptor.EligibleEvidence("EKS_EC2_HA", kind)
		if err != nil || !ok {
			t.Errorf("EligibleEvidence(EKS_EC2_HA, %s) = %v, %v; want true, nil", kind, ok, err)
		}
	}
}

func TestManifestCrossCheckDetectsDescriptorDrift(t *testing.T) {
	descriptor := realDescriptor(t)
	if err := CrossCheckManifests(descriptor, realManifestDir); err != nil {
		t.Fatalf("manifest cross-check: %v", err)
	}

	drifted := func(mutate func(q *Qualification)) Descriptor {
		copy := descriptor
		copy.Profiles = append([]Profile(nil), descriptor.Profiles...)
		for index := range copy.Profiles {
			if copy.Profiles[index].ID == "EKS_EC2_HA" {
				q := *copy.Profiles[index].Qualification
				mutate(&q)
				copy.Profiles[index].Qualification = &q
			}
		}
		return copy
	}
	manifestDrift := map[string]func(q *Qualification){
		"statefulset terminationGracePeriodSeconds": func(q *Qualification) { q.Rollout.TerminationGracePeriodSeconds = 700 },
		"statefulset updateStrategy.type":           func(q *Qualification) { q.Rollout.UpdateStrategy = "RollingUpdate" },
		"volumeClaimTemplate storage request":       func(q *Qualification) { q.Storage.SizeGiB = 100 },
		"nlb annotation target-type":                func(q *Qualification) { q.LoadBalancer.TargetType = "instance" },
		"broker memory limit":                       func(q *Qualification) { q.Broker.MemoryRequestsAndLimits = "8Gi" },
		"pdb maxUnavailable":                        func(q *Qualification) { q.Rollout.PodDisruptionBudgetMaxUnavailable = 2 },
	}
	for want, mutate := range manifestDrift {
		err := CrossCheckManifests(drifted(mutate), realManifestDir)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("manifest drift %q: error = %v", want, err)
		}
	}
	if err := CrossCheckManifests(descriptor, t.TempDir()); err == nil || !strings.Contains(err.Error(), "statefulset.yaml") {
		t.Errorf("missing manifest directory error = %v", err)
	}
}

func TestDecodeDescriptorRejectsInvalidDescriptors(t *testing.T) {
	cases := []struct {
		name    string
		data    func(t *testing.T) []byte
		wantErr string
	}{
		{"malformed JSON", func(*testing.T) []byte { return []byte(`{"schemaVersion":`) }, "decode deployment profile descriptor"},
		{"trailing data", func(t *testing.T) []byte { return append(realDescriptorBytes(t), []byte(` {}`)...) }, "unexpected data"},
		{"unknown top-level field", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { root["extra"] = true })
		}, "unknown field"},
		{"unknown profile field", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { profileNode(t, root, "ECS_EC2")["supportLevel"] = "gold" })
		}, "unknown field"},
		{"unknown qualification field", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "EKS_EC2_HA")["qualification"].(map[string]any)["extra"] = 1
			})
		}, "unknown field"},
		{"unknown status", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { profileNode(t, root, "ECS_EC2")["status"] = "Beta" })
		}, "status must be Supported, Experimental or Unsupported"},
		{"lowercase status", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { profileNode(t, root, "ECS_EC2")["status"] = "experimental" })
		}, "status must be"},
		{"duplicate profile", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				root["profiles"] = append(root["profiles"].([]any), profileNode(t, root, "ECS_EC2"))
			})
		}, "duplicate profile id"},
		{"missing EKS_EC2_HA", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { removeProfile(t, root, "EKS_EC2_HA") })
		}, "required profile EKS_EC2_HA is missing"},
		{"missing COMPOSE_LOCAL", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { removeProfile(t, root, "COMPOSE_LOCAL") })
		}, "required profile COMPOSE_LOCAL is missing"},
		{"unknown profile id", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { profileNode(t, root, "ECS_EC2")["id"] = "ECS_FARGATE" })
		}, "unknown profile id"},
		{"second Supported profile", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { profileNode(t, root, "ECS_EC2")["status"] = "Supported" })
		}, "only the reference profile EKS_EC2_HA may be Supported"},
		{"reference profile not Supported", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { profileNode(t, root, "EKS_EC2_HA")["status"] = "Experimental" })
		}, "reference profile must be Supported"},
		{"wrong reference profile", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { root["referenceProfile"] = "ECS_EC2" })
		}, "referenceProfile must be"},
		{"wrong schema version", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { root["schemaVersion"] = "v0" })
		}, "schemaVersion must be"},
		{"Unsupported profile claims HA evidence", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "COMPOSE_LOCAL")["evidenceEligibleFor"].(map[string]any)["ha"] = true
			})
		}, "evidenceEligibleFor.ha must be false for a non-reference profile"},
		{"Experimental profile claims RPO evidence", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "ECS_EC2")["evidenceEligibleFor"].(map[string]any)["rpo"] = true
			})
		}, "evidenceEligibleFor.rpo must be false"},
		{"reference profile loses rolling update evidence", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "EKS_EC2_HA")["evidenceEligibleFor"].(map[string]any)["rollingUpdate"] = false
			})
		}, "evidenceEligibleFor.rollingUpdate must be true for the reference profile"},
		{"missing evidence flag", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				delete(profileNode(t, root, "ECS_EC2")["evidenceEligibleFor"].(map[string]any), "rpo")
			})
		}, "evidenceEligibleFor.rpo is required"},
		{"empty scope", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { profileNode(t, root, "EKS_FARGATE")["scope"] = " " })
		}, "scope is required"},
		{"missing qualification", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) { delete(profileNode(t, root, "EKS_EC2_HA"), "qualification") })
		}, "qualification is required for the reference profile"},
		{"qualification on unsupported profile", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "ECS_EC2")["qualification"] = profileNode(t, root, "EKS_EC2_HA")["qualification"]
			})
		}, "qualification is only allowed on the reference profile"},
		{"malformed kubernetes minor", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "EKS_EC2_HA")["qualification"].(map[string]any)["kubernetes"].(map[string]any)["minor"] = "latest"
			})
		}, "kubernetes.minor must look like"},
		{"two availability zones", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "EKS_EC2_HA")["qualification"].(map[string]any)["topology"].(map[string]any)["availabilityZoneCount"] = 2
			})
		}, "availabilityZoneCount must be 3"},
		{"termination grace period below drain budget", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "EKS_EC2_HA")["qualification"].(map[string]any)["rollout"].(map[string]any)["terminationGracePeriodSeconds"] = 659
			})
		}, "terminationGracePeriodSeconds 659 must be at least"},
		{"unencrypted storage", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "EKS_EC2_HA")["qualification"].(map[string]any)["storage"].(map[string]any)["encrypted"] = false
			})
		}, "storage.encrypted must be true"},
		{"non-EC2 node group", func(t *testing.T) []byte {
			return mutatedDescriptor(t, func(root map[string]any) {
				profileNode(t, root, "EKS_EC2_HA")["qualification"].(map[string]any)["nodeGroup"].(map[string]any)["computeType"] = "FARGATE"
			})
		}, "nodeGroup.computeType must be EC2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeDescriptor(tc.data(t))
			if err == nil {
				t.Fatal("descriptor was accepted, want an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestEligibleEvidenceRejectsUnknownKind(t *testing.T) {
	descriptor := realDescriptor(t)
	if _, err := descriptor.EligibleEvidence("EKS_EC2_HA", "throughput"); err == nil || !strings.Contains(err.Error(), "unknown evidence kind") {
		t.Fatalf("unknown kind error = %v", err)
	}
	if _, _, err := descriptor.FilterEvidence([]Evidence{{Profile: "EKS_EC2_HA", Kind: "latency", Source: "x"}}); err == nil || !strings.Contains(err.Error(), `"x"`) {
		t.Fatalf("filter error = %v", err)
	}
}

func TestCLIFilterAndUsageErrors(t *testing.T) {
	dir := t.TempDir()
	evidence := filepath.Join(dir, "evidence.json")
	body := `[{"profile":"EKS_EC2_HA","kind":"rpo","source":"eks-run"},{"profile":"COMPOSE_LOCAL","kind":"ha","source":"compose-run"}]`
	if err := os.WriteFile(evidence, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"filter", "-descriptor", realDescriptorPath, "-evidence", evidence}, &stdout, &stderr); code != 0 {
		t.Fatalf("filter exit code %d: %s", code, stderr.String())
	}
	var result struct {
		Eligible []Evidence `json:"eligible"`
		Excluded []struct {
			Source string `json:"source"`
			Reason string `json:"reason"`
		} `json:"excluded"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("filter output: %v\n%s", err, stdout.String())
	}
	if len(result.Eligible) != 1 || result.Eligible[0].Source != "eks-run" {
		t.Errorf("eligible = %+v", result.Eligible)
	}
	if len(result.Excluded) != 1 || result.Excluded[0].Source != "compose-run" || !strings.Contains(result.Excluded[0].Reason, "Unsupported") {
		t.Errorf("excluded = %+v", result.Excluded)
	}

	unknownField := filepath.Join(dir, "unknown.json")
	if err := os.WriteFile(unknownField, []byte(`[{"profile":"EKS_EC2_HA","kind":"ha","source":"x","note":1}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	invalidDescriptor := filepath.Join(dir, "descriptor.json")
	if err := os.WriteFile(invalidDescriptor, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"no command":            {},
		"unknown command":       {"publish"},
		"missing descriptor":    {"validate"},
		"descriptor not found":  {"validate", "-descriptor", filepath.Join(dir, "absent.json")},
		"invalid descriptor":    {"validate", "-descriptor", invalidDescriptor},
		"missing evidence flag": {"filter", "-descriptor", realDescriptorPath},
		"evidence not found":    {"filter", "-descriptor", realDescriptorPath, "-evidence", filepath.Join(dir, "absent.json")},
		"evidence unknown key":  {"filter", "-descriptor", realDescriptorPath, "-evidence", unknownField},
		"manifest dir missing":  {"validate", "-descriptor", realDescriptorPath, "-manifests", dir},
	} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code == 0 {
			t.Errorf("%s: exit code 0, want failure (stdout %q)", name, out.String())
		} else if errOut.Len() == 0 {
			t.Errorf("%s: no error message written", name)
		}
	}
}
