package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Traceability: CFG-SCN-35~38; docs/specs/configuration-and-runtime-options.md §10; ADR-0052 §1.1.1.
// Scenarios: configuration-and-runtime-options.md:CFG-SCN-35, configuration-and-runtime-options.md:CFG-SCN-36, configuration-and-runtime-options.md:CFG-SCN-37, configuration-and-runtime-options.md:CFG-SCN-38
func TestDynamicClusterPolicyTenantQuotasProcessAcceptance(t *testing.T) {
	t.Parallel()

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	ctx := fixture.ctx
	legacyProfile := clusterTenantQuotaProcessProfile(1)
	legacyProfile.Tenants[0].RetainedBytes = 1 << 20
	configPaths := make([]string, 3)
	nodes := make([]*clusterMembershipProcess, 3)
	for index := range nodes {
		configPaths[index] = fixture.bootstrapConfig(uint64(index + 1))
		clusterTenantQuotaProcessSetProfile(t, configPaths[index], legacyProfile)
		process, err := fixture.harness.start(fmt.Sprintf("cluster tenant policy node %d", index+1), configPaths[index], fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start node %d: %v", index+1, err)
		}
		nodes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	for _, process := range nodes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
		clusterTenantQuotaProcessWaitForProfile(t, ctx, process, 1, false)
	}

	limits := clusterPolicyProcessMQTTLimits{
		MaxPacketSize: 8192, ReceiveMaximum: 64, ConnectTimeoutMillis: 10000,
		PacketReadTimeoutMillis: 30000, WriteTimeoutMillis: 5000,
	}
	policyQuotas := clusterPolicyProcessTenantQuotas{
		Default: legacyProfile.Default,
		Tenants: []clusterMembershipProcessTenantQuotaTenant{legacyProfile.Tenants[0]},
	}
	policyQuotas.Tenants[0].RetainedBytes = 4096
	quotaBody := clusterPolicyProcessBodyWithTenantQuotas(limits, &policyQuotas)

	beforeFeature := clusterPolicyProcessHeaders(`"0"`, "tenant-policy-before-feature")
	status, body, err := clusterMembershipProcessRequestWithHeader(ctx, http.MethodPut,
		nodes[0].managementURL()+"/api/v1/cluster/policy", quotaBody, beforeFeature)
	if err != nil || status != http.StatusConflict || !strings.Contains(string(body), `"code":"feature_inactive"`) {
		t.Fatalf("policy quota update before cluster-policy activation status=%d body=%s err=%v", status, body, err)
	}

	clusterProcessActivateFeature(t, ctx, nodes, "cluster-policy")
	status, body, err = clusterMembershipProcessRequestWithHeader(ctx, http.MethodPut,
		nodes[0].managementURL()+"/api/v1/cluster/policy", clusterPolicyProcessBody(limits), clusterPolicyProcessHeaders(`"0"`, "tenant-policy-mqtt-v1"))
	if err != nil || status != http.StatusOK {
		t.Fatalf("initial MQTT policy update status=%d body=%s err=%v", status, body, err)
	}
	if state := clusterPolicyProcessDecodeState(t, body); state.Version != 1 || state.Policy == nil {
		t.Fatalf("initial policy response=%+v, want version 1", state)
	}

	beforeQuotaFeature := clusterPolicyProcessHeaders(`"1"`, "tenant-policy-before-quota-feature")
	status, body, err = clusterMembershipProcessRequestWithHeader(ctx, http.MethodPut,
		nodes[0].managementURL()+"/api/v1/cluster/policy", quotaBody, beforeQuotaFeature)
	if err != nil || status != http.StatusConflict || !strings.Contains(string(body), `"code":"feature_inactive"`) {
		t.Fatalf("policy quota update before tenant quota feature status=%d body=%s err=%v", status, body, err)
	}

	clusterProcessActivateFeature(t, ctx, nodes, "cluster-policy-tenant-quotas")
	status, body, err = clusterMembershipProcessRequestWithHeader(ctx, http.MethodPut,
		nodes[0].managementURL()+"/api/v1/cluster/policy", quotaBody, clusterPolicyProcessHeaders(`"1"`, "tenant-policy-quota-v2"))
	if err != nil || status != http.StatusOK {
		t.Fatalf("policy tenant quota update status=%d body=%s err=%v", status, body, err)
	}
	state := clusterPolicyProcessDecodeState(t, body)
	if state.Version != 2 || state.Policy == nil || !reflect.DeepEqual(state.Policy.TenantQuotas, &policyQuotas) {
		t.Fatalf("policy quota update response=%+v, want committed version 2 and tenant quota values", state)
	}
	clusterPolicyProcessWaitForTenantQuotas(t, ctx, nodes, []uint64{1, 2, 3}, 2, limits, &policyQuotas, "clusterPolicy", 2, &policyQuotas)

	if err := nodes[2].stop(10 * time.Second); err != nil {
		t.Fatalf("stop node 3 to verify policy quota restore: %v", err)
	}
	restarted, err := fixture.harness.start("cluster tenant policy node 3 restart", configPaths[2], fixture.managementAddresses[2])
	if err != nil {
		t.Fatalf("restart node 3: %v", err)
	}
	nodes[2] = restarted
	clusterMembershipProcessWaitForBootstrap(t, ctx, nodes)
	clusterMembershipProcessWaitForStatus(t, ctx, restarted, "/health/ready", http.StatusOK)
	clusterPolicyProcessWaitForTenantQuotas(t, ctx, []*clusterMembershipProcess{restarted}, []uint64{3}, 2, limits, &policyQuotas, "clusterPolicy", 2, &policyQuotas)

	clusterTenantQuotaProcessExpectRetainedQuotaClose(
		t, fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher",
		"retained/quota/policy-rejected", strings.Repeat("x", 6000),
	)
	clusterTenantQuotaProcessWaitForMetric(t, ctx, nodes[0], `narumq_tenant_quota_rejections_total{tenant="acceptance",dimension="retained_bytes"} 1`)

	status, body, err = clusterMembershipProcessRequestWithHeader(ctx, http.MethodPut,
		nodes[0].managementURL()+"/api/v1/cluster/policy", clusterPolicyProcessBody(limits), clusterPolicyProcessHeaders(`"2"`, "tenant-policy-fallback-v3"))
	if err != nil || status != http.StatusOK {
		t.Fatalf("remove policy tenant quota update status=%d body=%s err=%v", status, body, err)
	}
	state = clusterPolicyProcessDecodeState(t, body)
	if state.Version != 3 || state.Policy == nil || state.Policy.TenantQuotas != nil {
		t.Fatalf("quota removal response=%+v, want version 3 without policy quotas", state)
	}
	legacyQuotas := clusterPolicyProcessTenantQuotas{Default: legacyProfile.Default, Tenants: legacyProfile.Tenants}
	clusterPolicyProcessWaitForTenantQuotas(t, ctx, nodes, []uint64{1, 2, 3}, 3, limits, nil, "replicatedProfile", 1, &legacyQuotas)

	if err := clusterMembershipProcessTryPublishWithRetain(
		fixture.mqttAddresses[0], fixture.accessToken, "retained-publisher",
		"retained/quota/policy-fallback", []byte(strings.Repeat("y", 6000)), true,
	); err != nil {
		t.Fatalf("legacy replicated tenant quota did not apply after policy quota removal: %v", err)
	}
}

type clusterPolicyProcessTenantQuotas struct {
	Default clusterMembershipProcessTenantQuotaLimits   `json:"default"`
	Tenants []clusterMembershipProcessTenantQuotaTenant `json:"tenants"`
}

func clusterPolicyProcessBodyWithTenantQuotas(limits clusterPolicyProcessMQTTLimits, quotas *clusterPolicyProcessTenantQuotas) []byte {
	body, _ := json.Marshal(struct {
		MQTTLimits   clusterPolicyProcessMQTTLimits    `json:"mqttLimits"`
		TenantQuotas *clusterPolicyProcessTenantQuotas `json:"tenantQuotas,omitempty"`
	}{MQTTLimits: limits, TenantQuotas: quotas})
	return body
}

type clusterPolicyProcessQuotaProfileResponse struct {
	Version uint64                                      `json:"version"`
	Default clusterMembershipProcessTenantQuotaLimits   `json:"default"`
	Tenants []clusterMembershipProcessTenantQuotaTenant `json:"tenants"`
}

func clusterPolicyProcessWaitForTenantQuotas(
	t *testing.T,
	ctx context.Context,
	processes []*clusterMembershipProcess,
	nodeIDs []uint64,
	version uint64,
	limits clusterPolicyProcessMQTTLimits,
	policyQuotas *clusterPolicyProcessTenantQuotas,
	effectiveSource string,
	effectiveVersion uint64,
	effectiveQuotas *clusterPolicyProcessTenantQuotas,
) {
	t.Helper()
	if len(processes) != len(nodeIDs) {
		t.Fatalf("process count %d does not match node ID count %d", len(processes), len(nodeIDs))
	}
	clusterMembershipProcessWait(t, ctx, fmt.Sprintf("cluster policy tenant quotas version %d", version), func() (bool, string, error) {
		for index, process := range processes {
			status, body, err := clusterMembershipProcessRequest(ctx, http.MethodGet,
				process.managementURL()+"/api/v1/cluster/policy", nil, clusterMembershipProcessAdmin, "")
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("%s GET status=%d body=%s err=%v", process.name, status, body, err), nil
			}
			state, decodeErr := clusterPolicyProcessDecodeStateValue(body)
			if decodeErr != nil {
				return false, process.name + " policy decode: " + decodeErr.Error(), nil
			}
			if state.Version != version || state.Policy == nil || state.Policy.MQTTLimits != limits ||
				!reflect.DeepEqual(state.Policy.TenantQuotas, policyQuotas) {
				return false, fmt.Sprintf("%s policy state=%+v, want version=%d quotas=%+v", process.name, state, version, policyQuotas), nil
			}

			status, body, err = clusterMembershipProcessRequest(ctx, http.MethodGet,
				fmt.Sprintf("%s/api/v1/nodes/%d/config", process.managementURL(), nodeIDs[index]),
				nil, clusterMembershipProcessAdmin, "")
			if err != nil || status != http.StatusOK {
				return false, fmt.Sprintf("%s node config status=%d body=%s err=%v", process.name, status, body, err), nil
			}
			var configuration struct {
				Data struct {
					EffectivePolicy struct {
						TenantQuotas struct {
							FeatureActive bool                                      `json:"featureActive"`
							Source        string                                    `json:"source"`
							Version       *uint64                                   `json:"version"`
							Value         *clusterPolicyProcessQuotaProfileResponse `json:"value"`
						} `json:"tenantQuotas"`
					} `json:"effectivePolicy"`
				} `json:"data"`
			}
			if err := json.Unmarshal(body, &configuration); err != nil {
				return false, fmt.Sprintf("%s node config decode: %v body=%s", process.name, err, body), nil
			}
			effective := configuration.Data.EffectivePolicy.TenantQuotas
			if effective.Version == nil || *effective.Version != effectiveVersion || !effective.FeatureActive ||
				effective.Source != effectiveSource || effective.Value == nil ||
				effective.Value.Version != effectiveVersion ||
				!reflect.DeepEqual(effective.Value.Default, effectiveQuotas.Default) ||
				!reflect.DeepEqual(effective.Value.Tenants, effectiveQuotas.Tenants) {
				return false, fmt.Sprintf("%s effective tenant quotas=%+v, want source=%s version=%d value=%+v",
					process.name, effective, effectiveSource, effectiveVersion, effectiveQuotas), nil
			}
		}
		return true, fmt.Sprintf("all %d nodes report policy and effective quota state at version %d", len(processes), version), nil
	})
}
