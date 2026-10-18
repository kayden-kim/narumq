package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	adaptersmetrics "narumq/internal/adapters/metrics"
	"narumq/internal/application/membership"
)

// Traceability: API-09; CFG-SCN-26; ADR-0052 §1.1.1.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-26
func TestClusterPolicyReconciliationDecision(t *testing.T) {
	configured := validClusterPolicyConfig()
	policy := configured.membershipPolicy()
	committed := &membership.ClusterPolicyStateDTO{Version: 1, Policy: policy}
	changed := *configured
	changed.Version = 2
	changed.MQTTLimits = &clusterMQTTLimitsConfig{
		MaxPacketSize: 2_097_152, ReceiveMaximum: 32,
		ConnectTimeoutMillis: 10_000, PacketReadTimeoutMillis: 30_000, WriteTimeoutMillis: 5_000,
	}
	mismatch := *configured
	mismatch.MQTTLimits = &clusterMQTTLimitsConfig{
		MaxPacketSize: 2_097_152, ReceiveMaximum: 64,
		ConnectTimeoutMillis: 10_000, PacketReadTimeoutMillis: 30_000, WriteTimeoutMillis: 5_000,
	}

	type testCase struct {
		name                     string
		desired                  *clusterPolicyConfig
		committed                *membership.ClusterPolicyStateDTO
		featureActive            bool
		tenantQuotaFeatureActive bool
		wantProposal             bool
		wantMismatch             bool
		wantExpected             uint64
		wantKey                  string
	}
	tests := []testCase{
		{name: "feature inactive", desired: configured},
		{name: "no state or local config", featureActive: true},
		{name: "initial proposal", desired: configured, featureActive: true, wantProposal: true, wantKey: "config-v1"},
		{name: "higher configured version", desired: &changed, committed: committed, featureActive: true, wantProposal: true, wantExpected: 1, wantKey: "config-v2"},
		{name: "lower configured version", desired: configured, committed: &membership.ClusterPolicyStateDTO{Version: 2, Policy: policy}, featureActive: true, wantMismatch: true},
		{name: "local config absent", committed: committed, featureActive: true, wantMismatch: true},
		{name: "matching version and content", desired: configured, committed: committed, featureActive: true},
		{name: "same version content mismatch", desired: &mismatch, committed: committed, featureActive: true, wantMismatch: true},
	}
	withTenantQuotas := *configured
	withTenantQuotas.TenantQuotas = validClusterPolicyTenantQuotasConfig()
	tests = append(tests,
		testCase{name: "tenant quota feature inactive", desired: &withTenantQuotas, featureActive: true, wantMismatch: true},
		testCase{name: "tenant quota feature active", desired: &withTenantQuotas, featureActive: true, tenantQuotaFeatureActive: true, wantProposal: true, wantKey: "config-v1"},
	)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proposal, gotMismatch := clusterPolicyReconciliationDecision(
				test.desired, test.committed, test.featureActive, test.tenantQuotaFeatureActive,
			)
			if (proposal != nil) != test.wantProposal || gotMismatch != test.wantMismatch {
				t.Fatalf("decision = (%+v, mismatch=%t), want proposal=%t mismatch=%t", proposal, gotMismatch, test.wantProposal, test.wantMismatch)
			}
			if proposal != nil && (proposal.ExpectedVersion != test.wantExpected || proposal.IdempotencyKey != test.wantKey) {
				t.Fatalf("proposal = %+v, want expected version %d and key %q", proposal, test.wantExpected, test.wantKey)
			}
		})
	}
}

// Traceability: API-09; CFG-SCN-26; ADR-0052 §1.1.1.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-26
func TestEffectiveClusterMQTTLimitsPrecedence(t *testing.T) {
	defaults := defaultClusterMQTTLimits()
	config := clusterConfigFile{}
	got, source, version := effectiveClusterMQTTLimits(config, nil, false)
	if got != defaults || source != clusterMQTTLimitsSourceDefault || version != nil {
		t.Fatalf("default limits = (%+v, %q, %v), want (%+v, default, nil)", got, source, version, defaults)
	}

	config.ClusterPolicy = validClusterPolicyConfig()
	local := config.ClusterPolicy.membershipPolicy().MQTTLimits
	got, source, version = effectiveClusterMQTTLimits(config, nil, true)
	if got != local || source != clusterMQTTLimitsSourceLocalConfig || version != nil {
		t.Fatalf("local limits = (%+v, %q, %v), want (%+v, localConfig, nil)", got, source, version, local)
	}

	committed := membership.ClusterPolicyStateDTO{Version: 2, Policy: membership.ClusterPolicyDTO{MQTTLimits: membership.MQTTLimitsDTO{
		MaxPacketSize: 2_097_152, ReceiveMaximum: 32,
		ConnectTimeoutMillis: 15_000, PacketReadTimeoutMillis: 45_000, WriteTimeoutMillis: 7_000,
	}}}
	got, source, version = effectiveClusterMQTTLimits(config, &committed, true)
	if got != committed.Policy.MQTTLimits || source != clusterMQTTLimitsSourceClusterPolicy || version == nil || *version != committed.Version {
		t.Fatalf("committed limits = (%+v, %q, %v), want (%+v, clusterPolicy, %d)", got, source, version, committed.Policy.MQTTLimits, committed.Version)
	}
	got, source, version = effectiveClusterMQTTLimits(config, &committed, false)
	if got != local || source != clusterMQTTLimitsSourceLocalConfig || version != nil {
		t.Fatalf("feature-inactive limits = (%+v, %q, %v), want (%+v, localConfig, nil)", got, source, version, local)
	}
}

func clusterPolicyReconciliationTestMachine(t *testing.T, policy membership.ClusterPolicyDTO) *membership.StateMachine {
	t.Helper()
	members := []membership.MemberIdentityDTO{
		{MemberID: 1, ClusterID: "cluster-a", Address: "node1.example:19001", DNSSAN: "node1.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter, Features: []string{membership.ClusterPolicyFeature}},
		{MemberID: 2, ClusterID: "cluster-a", Address: "node2.example:19002", DNSSAN: "node2.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter, Features: []string{membership.ClusterPolicyFeature}},
		{MemberID: 3, ClusterID: "cluster-a", Address: "node3.example:19003", DNSSAN: "node3.example", CertificateSHA256: make([]byte, 32), ReleaseVersion: "1.2.3", ClusterProtocolMajor: 2, StateSchemaMajor: 1, Phase: membership.MemberPhaseVoter, Features: []string{membership.ClusterPolicyFeature}},
	}
	machine := membership.NewStateMachine()
	if err := machine.InitializeBootstrap(1, members); err != nil {
		t.Fatalf("initialize policy machine: %v", err)
	}
	activate := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID:       "00000000-0000-4000-8000-000000000001",
		FeatureActivation: &membership.FeatureActivationDTO{Feature: membership.ClusterPolicyFeature},
	}
	if result, err := machine.Apply(2, activate); err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("activate ClusterPolicy feature: result=%+v err=%v", result, err)
	}
	change := membership.ClusterPolicyChangeDTO{
		ExpectedVersion: 0, Version: 1, Policy: policy, IdempotencyKey: "committed-policy",
	}
	apply := membership.CommandDTO{
		SchemaMajor: membership.SchemaMajor, SchemaMinor: membership.SchemaMinor,
		OperationID: "00000000-0000-4000-8000-000000000002", ClusterPolicy: &change,
	}
	if result, err := machine.Apply(3, apply); err != nil || result.Outcome != membership.ApplyOutcomeApplied {
		t.Fatalf("apply committed policy: result=%+v err=%v", result, err)
	}
	return machine
}

// Traceability: API-09; CFG-SCN-26; docs/specs/management-api-and-observability.md §7.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-26, management-api-and-observability.md:MGT-SCN-039
func TestClusterPolicyControllerReconcilesAndLogsMismatchOnce(t *testing.T) {
	var logs bytes.Buffer
	metrics := adaptersmetrics.NewRegistry()
	configured := validClusterPolicyConfig()
	configured.MQTTLimits.ReceiveMaximum = 32
	committedPolicy := validClusterPolicyConfig().membershipPolicy()
	machine := clusterPolicyReconciliationTestMachine(t, committedPolicy)
	runtime := &clusterRuntime{
		config: clusterConfigFile{ClusterPolicy: configured}, machine: machine,
		logger: slog.New(slog.NewTextHandler(&logs, nil)), metrics: metrics,
	}
	controller := &clusterPolicyController{runtime: runtime}
	controller.reconcile(context.Background())
	controller.reconcile(context.Background())

	state, found := machine.ClusterPolicy()
	if !found || state.Version != 1 || !membership.EqualClusterPolicy(state.Policy, committedPolicy) {
		t.Fatalf("reconciliation changed committed policy: state=%+v found=%t", state, found)
	}
	if got := strings.Count(logs.String(), "committed cluster policy differs from local configuration"); got != 1 {
		t.Fatalf("mismatch warning count = %d, want 1; logs=%q", got, logs.String())
	}
	for _, expected := range []string{
		"narumq_cluster_policy_committed 1\n",
		"narumq_cluster_policy_version 1\n",
		"narumq_cluster_policy_config_mismatch 1\n",
	} {
		if !bytes.Contains(metrics.Render(false), []byte(expected)) {
			t.Errorf("metrics omit %q", expected)
		}
	}
}
