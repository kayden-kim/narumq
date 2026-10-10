package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	adapmetrics "narumq/internal/adapters/metrics"
)

type dashboardStackDefinition struct {
	UID        string                `json:"uid"`
	Title      string                `json:"title"`
	Panels     []dashboardStackPanel `json:"panels"`
	Templating struct {
		List []dashboardStackVariable `json:"list"`
	} `json:"templating"`
}

type dashboardStackPanel struct {
	ID              int                       `json:"id"`
	Title           string                    `json:"title"`
	Type            string                    `json:"type"`
	Targets         []dashboardStackTarget    `json:"targets"`
	Transformations []dashboardStackTransform `json:"transformations"`
	FieldConfig     json.RawMessage           `json:"fieldConfig"`
}

type dashboardStackColumn struct {
	Selector string `json:"selector"`
	Text     string `json:"text"`
	Type     string `json:"type"`
}

type dashboardStackTarget struct {
	Datasource struct {
		Type string `json:"type"`
		UID  string `json:"uid"`
	} `json:"datasource"`
	Type       string `json:"type"`
	Source     string `json:"source"`
	URL        string `json:"url"`
	URLOptions struct {
		Method string `json:"method"`
	} `json:"url_options"`
	Expr         string                 `json:"expr"`
	EditorMode   string                 `json:"editorMode"`
	Format       string                 `json:"format"`
	RootSelector string                 `json:"root_selector"`
	JSONOptions  json.RawMessage        `json:"json_options"`
	Instant      bool                   `json:"instant"`
	LegendFormat string                 `json:"legendFormat"`
	Range        bool                   `json:"range"`
	RefID        string                 `json:"refId"`
	Columns      []dashboardStackColumn `json:"columns"`
	Parser       string                 `json:"parser"`
}

type dashboardStackTransform struct {
	ID      string `json:"id"`
	Options struct {
		KeepLabels   []string          `json:"keepLabels"`
		ColumnField  string            `json:"columnField"`
		RowField     string            `json:"rowField"`
		ValueField   string            `json:"valueField"`
		RenameByName map[string]string `json:"renameByName"`
		Conversions  []struct {
			TargetField     string `json:"targetField"`
			DestinationType string `json:"destinationType"`
		} `json:"conversions"`
		Sort []struct {
			Field string `json:"field"`
			Desc  bool   `json:"desc"`
		} `json:"sort"`
	} `json:"options"`
}
type dashboardStackQueryResponse struct {
	Results map[string]dashboardStackQueryResult `json:"results"`
}

type dashboardStackQueryResult struct {
	Status int                        `json:"status"`
	Error  string                     `json:"error"`
	Frames []dashboardStackQueryFrame `json:"frames"`
}

type dashboardStackQueryFrame struct {
	Schema struct {
		Fields []struct {
			Name   string            `json:"name"`
			Type   string            `json:"type"`
			Labels map[string]string `json:"labels"`
		} `json:"fields"`
	} `json:"schema"`
	Data struct {
		Values [][]any `json:"values"`
	} `json:"data"`
}

type dashboardStackVariable struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Multi      bool   `json:"multi"`
	IncludeAll bool   `json:"includeAll"`
	AllValue   string `json:"allValue"`
	Current    struct {
		Text  string `json:"text"`
		Value any    `json:"value"`
	} `json:"current"`
	Query json.RawMessage `json:"query"`
}

// TestDashboardProvisioningMatchesContracts checks panel, query, datasource, and provisioning contracts.
// Traceability: UI-01, UI-02, UI-03, UI-04, UI-05, UI-10; docs/specs/management-api-and-observability.md §8.
func TestDashboardProvisioningMatchesContracts(t *testing.T) {
	root := clusterMembershipProcessRepoRoot(t)
	read := func(relative string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("read %s: %v", relative, err)
		}
		return data
	}

	var dashboard dashboardStackDefinition
	if err := json.Unmarshal(read("deploy/observability/grafana/dashboards/narumq-operations.json"), &dashboard); err != nil {
		t.Fatalf("decode Grafana dashboard: %v", err)
	}
	if dashboard.UID != "narumq-operations" || dashboard.Title != "NaruMQ Operations" {
		t.Fatalf("dashboard identity = %q/%q", dashboard.UID, dashboard.Title)
	}

	expectedPanels := map[int]struct{ title, panelType string }{
		1:  {"Quorum", "stat"},
		2:  {"Replica 부족", "stat"},
		3:  {"Leader", "stat"},
		4:  {"Member·Quorum", "stat"},
		5:  {"Node 상태 수", "stat"},
		6:  {"Node topology", "table"},
		7:  {"연결·session·subscription", "timeseries"},
		8:  {"Message rate", "timeseries"},
		9:  {"Protocol별 CONNECT 성공", "timeseries"},
		10: {"Protocol별 CONNECT 거절", "timeseries"},
		11: {"Drop·거절", "timeseries"},
		12: {"Node별 연결 편차", "table"},
		13: {"Node별 연결 추이", "timeseries"},
		14: {"Drain 진행", "table"},
		15: {"Drain 상세 (${drain_node})", "table"},
	}
	panels := make(map[int]dashboardStackPanel, len(expectedPanels))
	for _, panel := range dashboard.Panels {
		if panel.Type == "row" {
			continue
		}
		if _, duplicate := panels[panel.ID]; duplicate {
			t.Fatalf("duplicate dashboard panel ID %d", panel.ID)
		}
		panels[panel.ID] = panel
	}
	if len(panels) != len(expectedPanels) {
		t.Fatalf("dashboard data panels = %d, want %d", len(panels), len(expectedPanels))
	}
	spec := string(read("docs/specs/management-api-and-observability.md"))
	for id, expected := range expectedPanels {
		panel, ok := panels[id]
		if !ok || panel.Title != expected.title || panel.Type != expected.panelType {
			t.Fatalf("panel %d = (%q, %q), want (%q, %q)", id, panel.Title, panel.Type, expected.title, expected.panelType)
		}
		rowPrefix := fmt.Sprintf("| %d |", id)
		foundSpecRow := false
		for _, line := range strings.Split(spec, "\n") {
			if strings.HasPrefix(line, rowPrefix) && strings.Contains(line, expected.title) {
				foundSpecRow = true
				break
			}
		}
		if !foundSpecRow {
			t.Errorf("panel %d title %q is not listed in the dashboard contract", id, expected.title)
		}
	}
	quorumTargets := panels[1].Targets
	if len(quorumTargets) != 1 || len(quorumTargets[0].Columns) != 1 ||
		quorumTargets[0].Columns[0].Selector != "quorumAvailable" || quorumTargets[0].Columns[0].Type != "boolean" ||
		quorumTargets[0].Parser != "backend" {
		t.Errorf("quorum stat must parse quorumAvailable as a boolean: %+v", quorumTargets)
	}
	quorumTransforms := panels[1].Transformations
	if len(quorumTransforms) != 1 || quorumTransforms[0].ID != "convertFieldType" ||
		len(quorumTransforms[0].Options.Conversions) != 1 ||
		quorumTransforms[0].Options.Conversions[0].TargetField != "Quorum" ||
		quorumTransforms[0].Options.Conversions[0].DestinationType != "number" {
		t.Errorf("quorum stat must convert its boolean field to a number: %+v", quorumTransforms)
	}
	leaderTargets := panels[3].Targets
	if len(leaderTargets) != 1 || len(leaderTargets[0].Columns) != 1 ||
		leaderTargets[0].Columns[0].Selector != "leaderNodeId" || leaderTargets[0].Columns[0].Type != "number" ||
		leaderTargets[0].Parser != "backend" {
		t.Errorf("leader stat must parse leaderNodeId as a number: %+v", leaderTargets)
	}

	var openAPI struct {
		Paths map[string]map[string]json.RawMessage `json:"paths"`
	}
	if err := json.Unmarshal(read("docs/contracts/management-api-v1.openapi.json"), &openAPI); err != nil {
		t.Fatalf("decode management OpenAPI contract: %v", err)
	}
	apiTargets := 0
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			if target.Datasource.UID != "narumq-management" {
				continue
			}
			apiTargets++
			if target.Parser != "backend" {
				t.Errorf("panel %d Management API parser=%q, want backend", panel.ID, target.Parser)
			}
			if len(target.JSONOptions) != 0 {
				t.Errorf("panel %d backend Management API query has unused json_options=%s", panel.ID, target.JSONOptions)
			}
			if target.URLOptions.Method != http.MethodGet {
				t.Errorf("panel %d management query method = %q, want GET", panel.ID, target.URLOptions.Method)
			}
			path := strings.Split(target.URL, "?")[0]
			path = strings.ReplaceAll(path, "${drain_node}", "{nodeId}")
			if _, ok := openAPI.Paths[path]["get"]; !ok {
				t.Errorf("panel %d management path %q has no GET operation in OpenAPI", panel.ID, path)
			}
		}
	}
	if apiTargets != 7 {
		t.Errorf("dashboard Management API query count = %d, want 7", apiTargets)
	}

	variables := make(map[string]dashboardStackVariable, len(dashboard.Templating.List))
	for _, variable := range dashboard.Templating.List {
		variables[variable.Name] = variable
	}
	if len(variables) != 3 {
		t.Fatalf("dashboard variable count = %d, want 3", len(variables))
	}
	if node := variables["node"]; node.Type != "query" || !node.Multi || !node.IncludeAll || node.AllValue != ".*" || node.Current.Text != "All" || !bytes.Contains(node.Query, []byte("label_values(narumq_connections_active, node_id)")) {
		t.Errorf("node variable does not match the dashboard contract: %+v", node)
	}
	if drainNode := variables["drain_node"]; drainNode.Type != "query" || drainNode.Multi || !bytes.Contains(drainNode.Query, []byte("label_values(narumq_node_drain_active, node_id)")) {
		t.Errorf("drain_node variable does not match the dashboard contract: %+v", drainNode)
	}
	if deviation := variables["rebalance_max_deviation"]; deviation.Type != "textbox" || deviation.Current.Value != "100" {
		t.Errorf("rebalance_max_deviation default = %+v, want textbox value 100", deviation)
	}

	registryFamilies := make(map[string]struct{})
	for _, line := range strings.Split(string(adapmetrics.NewRegistry().Render(false)), "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				registryFamilies[fields[2]] = struct{}{}
			}
		}
	}
	metricPattern := regexp.MustCompile(`\bnarumq_[a-zA-Z0-9_]+\b`)
	queriedMetrics := make(map[string]struct{})
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			if target.Datasource.UID != "narumq-prometheus" {
				continue
			}
			if !strings.Contains(target.Expr, `node_id=~"$node"`) {
				t.Errorf("panel %d Prometheus query does not filter by node: %s", panel.ID, target.Expr)
			}
			for _, metric := range metricPattern.FindAllString(target.Expr, -1) {
				queriedMetrics[metric] = struct{}{}
				if !strings.Contains(spec, "`"+metric+"`") {
					t.Errorf("panel %d metric %q is absent from the observability contract", panel.ID, metric)
				}
				if _, ok := registryFamilies[metric]; !ok {
					t.Errorf("panel %d metric %q is absent from the exported registry", panel.ID, metric)
				}
			}
		}
	}
	if len(queriedMetrics) == 0 {
		t.Fatal("dashboard has no Prometheus metric queries")
	}
	for _, id := range []int{12, 14} {
		transformations := panels[id].Transformations
		expectedCount := 4
		if id == 12 {
			expectedCount = 5
		}
		if len(transformations) != expectedCount ||
			transformations[0].ID != "labelsToFields" || len(transformations[0].Options.KeepLabels) != 2 ||
			transformations[0].Options.KeepLabels[0] != "node_id" || transformations[0].Options.KeepLabels[1] != "metric" ||
			transformations[1].ID != "merge" ||
			transformations[2].ID != "groupingToMatrix" ||
			transformations[2].Options.ColumnField != "metric" ||
			transformations[2].Options.RowField != "node_id" ||
			transformations[2].Options.ValueField != "Value" ||
			transformations[3].ID != "organize" ||
			transformations[3].Options.RenameByName[`node_id\metric`] != "Node" {
			t.Errorf("panel %d must pivot metric labels into named node rows: %+v", id, transformations)
		}
		if id == 12 && (transformations[4].ID != "sortBy" ||
			len(transformations[4].Options.Sort) != 1 ||
			transformations[4].Options.Sort[0].Field != "deviation" ||
			!transformations[4].Options.Sort[0].Desc) {
			t.Errorf("panel 12 must sort nodes by descending deviation: %+v", transformations)
		}
		if len(panels[id].Targets) != 1 {
			t.Errorf("panel %d must use one unified Prometheus query, got %d", id, len(panels[id].Targets))
		}
	}
	for panelID, required := range map[int][]string{
		1:  {`"0"`, `"1"`, `위험`, `정상`, `red`, `green`},
		2:  {`"value":1`, `red`, `green`},
		3:  {`"match":"null"`, `없음`, `red`},
		6:  {`READY`, `JOINING`, `DRAINING`, `SAFE_TO_STOP`, `UNREACHABLE`},
		12: {`rebalance_needed`, `필요`, `정상`, `red`, `green`},
	} {
		var encoded []byte
		encoded, _ = json.Marshal(panels[panelID].FieldConfig)
		for _, fragment := range required {
			if !bytes.Contains(encoded, []byte(fragment)) {
				t.Errorf("panel %d field mapping lacks %q", panelID, fragment)
			}
		}
	}

	targetsBytes := read("deploy/observability/prometheus/targets/narumq-cluster.json")
	var targetGroups []struct {
		Targets []string          `json:"targets"`
		Labels  map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(targetsBytes, &targetGroups); err != nil {
		t.Fatalf("decode Prometheus target example: %v", err)
	}
	if len(targetGroups) != 3 {
		t.Fatalf("Prometheus target groups = %d, want 3", len(targetGroups))
	}
	for index, group := range targetGroups {
		if len(group.Targets) != 1 || group.Labels["cluster_id"] != "sample-cluster" || group.Labels["node_id"] != strconv.Itoa(index+1) {
			t.Errorf("Prometheus target group %d is not node-labeled: %+v", index, group)
		}
	}
	promConfig := string(read("deploy/observability/prometheus/prometheus.yml"))
	for _, required := range []string{"scrape_interval: 15s", "job_name: narumq-management", "metrics_path: /metrics", "/etc/prometheus/targets/*.json", "refresh_interval: 30s", "X-API-Key:", "/etc/prometheus/secrets/narumq-readonly.key"} {
		if !strings.Contains(promConfig, required) {
			t.Errorf("Prometheus config lacks %q", required)
		}
	}
	datasources := string(read("deploy/observability/grafana/provisioning/datasources/narumq.yml"))
	for _, required := range []string{"uid: narumq-prometheus", "url: http://prometheus:9090", "uid: narumq-management", "type: yesoreyeram-infinity-datasource", "url: ${NARUMQ_MANAGEMENT_URL}", "httpHeaderName1: X-API-Key", "httpHeaderValue1: ${NARUMQ_READONLY_API_KEY}", "allowDangerousHTTPMethods: false"} {
		if !strings.Contains(datasources, required) {
			t.Errorf("Grafana datasource provisioning lacks %q", required)
		}
	}
	provider := string(read("deploy/observability/grafana/provisioning/dashboards/narumq.yml"))
	if !strings.Contains(provider, "path: /var/lib/grafana/dashboards/narumq") || !strings.Contains(provider, "disableDeletion: true") {
		t.Error("Grafana dashboard provider does not provision the read-only operations dashboard")
	}
}

// TestDashboardStackProcessAcceptance checks the provisioned dashboard against three broker processes.
// Traceability: UI-01, UI-02, UI-03, UI-04, UI-05, UI-10; docs/specs/management-api-and-observability.md §8.
func TestDashboardStackProcessAcceptance(t *testing.T) {
	if os.Getenv("NARUMQ_DASHBOARD_E2E") != "1" {
		t.Skip("set NARUMQ_DASHBOARD_E2E=1 to run the Docker-backed Grafana and Prometheus acceptance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}").CombinedOutput(); err != nil {
		t.Skipf("Docker daemon is unavailable: %v: %s", err, strings.TrimSpace(string(output)))
	}

	fixture := newClusterMembershipProcessFixture(t, 3, "")
	fixture.managementBindAll = true
	readOnlyKeyBytes := make([]byte, 32)
	if _, err := rand.Read(readOnlyKeyBytes); err != nil {
		t.Fatalf("generate dashboard ReadOnly API key: %v", err)
	}
	fixture.readOnlyAPIKeyPath = filepath.Join(fixture.tempDir, "dashboard-readonly.key")
	if err := os.WriteFile(fixture.readOnlyAPIKeyPath, []byte(hex.EncodeToString(readOnlyKeyBytes)), 0o600); err != nil {
		t.Fatalf("write dashboard ReadOnly API key: %v", err)
	}
	fixture.managementAPIKeys = []clusterMembershipProcessAPIKey{{
		ID: "dashboard", KeyFile: fixture.readOnlyAPIKeyPath, Roles: []string{"ReadOnly"},
	}}

	processes := make([]*clusterMembershipProcess, 3)
	for index := range processes {
		configPath := fixture.bootstrapConfig(uint64(index + 1))
		clusterTenantQuotaProcessSetProfile(t, configPath, clusterTenantQuotaProcessProfile(1))
		process, err := fixture.harness.start(fmt.Sprintf("dashboard node %d", index+1), configPath, fixture.managementAddresses[index])
		if err != nil {
			t.Fatalf("start dashboard node %d: %v", index+1, err)
		}
		processes[index] = process
	}
	clusterMembershipProcessWaitForBootstrap(t, fixture.ctx, processes)
	clusterMetricsProcessWaitForLine(t, fixture.ctx, processes, "narumq_tenant_quota_profile_committed 1")
	dashboardStackWaitForReadyNodes(t, fixture.ctx, processes)

	stack := newDashboardStack(t, fixture)
	t.Cleanup(stack.cleanup)
	stack.start()
	stack.waitForPrometheusTargets()
	stack.assertPrometheusDrainSeries()
	stack.waitForGrafanaDashboard()

	dashboardURL := stack.grafanaURL + "/d/narumq-operations/narumq-operations"
	t.Logf("Grafana dashboard URL: %s", dashboardURL)
	if outputPath := os.Getenv("NARUMQ_DASHBOARD_E2E_URL_FILE"); outputPath != "" {
		if err := os.WriteFile(outputPath, []byte(dashboardURL), 0o600); err != nil {
			t.Fatalf("write Grafana dashboard URL file: %v", err)
		}
	}
	stack.assertGrafanaDatasourceHealth("narumq-prometheus")
	stack.assertGrafanaDatasourceHealth("narumq-management")
	stack.assertGrafanaDatasourceSet()
	stack.assertGrafanaDashboardHas15Panels()
	stack.assertGrafanaClusterOverview(processes[0])
	stack.assertGrafanaNodeTopology(processes[0])

	dashboardStackGenerateTraffic(t, fixture, processes)
	stack.assertGrafanaTrafficQueries()
	stack.assertGrafanaLoadAndDrainQueries()
	stack.assertManagementReadOnlyKey(processes[0])

	_, leaderURL := clusterMembershipProcessFindLeader(t, fixture.ctx, processes, 3)
	drainRequest := clusterDrainProcessRequest{
		ServerReference: fixture.mqttAddresses[0], ConnectionRatePerSecond: 1,
		LoadBalancerPropagationDelayMillis: 1000, TimeoutMillis: 60000,
	}
	status, body, err := clusterDrainProcessStart(fixture.ctx, leaderURL, 3, drainRequest, "dashboard-drain-node-3")
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("start node 3 dashboard drain status=%d body=%s err=%v", status, body, err)
	}
	initial, err := clusterDrainProcessDecodeOperation(body)
	if err != nil || initial.NodeID != "3" || initial.State != "DRAINING" {
		t.Fatalf("initial node 3 drain operation=%+v err=%v body=%s, want node 3 in DRAINING state", initial, err, body)
	}
	completed := clusterDrainProcessWaitForPhase(t, fixture.ctx, processes[0], 3, "COMPLETED")
	if completed.State != "SAFE_TO_STOP" || !completed.SafeToStop ||
		completed.Connections.Initial < 1 || completed.Connections.Remaining != 0 {
		t.Fatalf("completed node 3 dashboard drain=%+v, want safe-to-stop and zero remaining connections", completed)
	}
	stack.assertManagementDrainOperation(processes[0], completed)
	dashboardStackWait(t, fixture.ctx, 30*time.Second)
	stack.assertGrafanaDrainPanels()
	if hold := os.Getenv("NARUMQ_DASHBOARD_E2E_HOLD"); hold != "" {
		duration, err := time.ParseDuration(hold)
		if err != nil || duration <= 0 {
			t.Fatalf("parse NARUMQ_DASHBOARD_E2E_HOLD %q: %v", hold, err)
		}
		t.Logf("holding Grafana stack for %s", duration)
		time.Sleep(duration)
	}
}
func dashboardStackWaitForReadyNodes(t *testing.T, ctx context.Context, processes []*clusterMembershipProcess) {
	t.Helper()
	for _, process := range processes {
		clusterMembershipProcessWaitForStatus(t, ctx, process, "/health/ready", http.StatusOK)
	}
}

type dashboardStackTargetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

type dashboardDockerStack struct {
	t              *testing.T
	fixture        *clusterMembershipProcessFixture
	network        string
	prometheus     string
	grafana        string
	prometheusURL  string
	grafanaURL     string
	adminPassword  string
	readOnlyAPIKey string
}

func newDashboardStack(t *testing.T, fixture *clusterMembershipProcessFixture) *dashboardDockerStack {
	t.Helper()
	ports := clusterMembershipProcessAddresses(t, 2)
	promHost, promPort, err := net.SplitHostPort(ports[0])
	if err != nil {
		t.Fatalf("split Prometheus port: %v", err)
	}
	grafanaHost, grafanaPort, err := net.SplitHostPort(ports[1])
	if err != nil {
		t.Fatalf("split Grafana port: %v", err)
	}
	if promHost != "127.0.0.1" || grafanaHost != "127.0.0.1" {
		t.Fatalf("dashboard stack test requires loopback-published ports, got %q and %q", promHost, grafanaHost)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	stack := &dashboardDockerStack{
		t: t, fixture: fixture,
		network:       "narumq-dashboard-" + suffix,
		prometheus:    "narumq-dashboard-prom-" + suffix,
		grafana:       "narumq-dashboard-grafana-" + suffix,
		prometheusURL: "http://127.0.0.1:" + promPort,
		grafanaURL:    "http://127.0.0.1:" + grafanaPort,
		adminPassword: "dashboard-acceptance-admin",
	}
	return stack
}

func (s *dashboardDockerStack) start() {
	s.t.Helper()
	root := s.fixture.repoRoot
	stage := filepath.Join(s.fixture.tempDir, "dashboard-docker-stack")
	prometheusDir := filepath.Join(stage, "prometheus")
	prometheusTargetsDir := filepath.Join(prometheusDir, "targets")
	prometheusSecretsDir := filepath.Join(prometheusDir, "secrets")
	grafanaDir := filepath.Join(stage, "grafana")
	grafanaProvisioningDir := filepath.Join(grafanaDir, "provisioning")
	grafanaDashboardsDir := filepath.Join(grafanaDir, "dashboards")
	for _, directory := range []string{prometheusTargetsDir, prometheusSecretsDir, filepath.Join(grafanaProvisioningDir, "datasources"), filepath.Join(grafanaProvisioningDir, "dashboards"), filepath.Join(grafanaDashboardsDir, "narumq")} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			s.t.Fatalf("create Docker stack staging directory: %v", err)
		}
	}
	copy := func(source, destination string, mode os.FileMode) {
		s.t.Helper()
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(source)))
		if err != nil {
			s.t.Fatalf("read Docker stack artifact %s: %v", source, err)
		}
		if err := os.WriteFile(destination, data, mode); err != nil {
			s.t.Fatalf("stage Docker stack artifact %s: %v", source, err)
		}
	}
	copy("deploy/observability/prometheus/prometheus.yml", filepath.Join(prometheusDir, "prometheus.yml"), 0o644)
	copy("deploy/observability/grafana/provisioning/datasources/narumq.yml", filepath.Join(grafanaProvisioningDir, "datasources", "narumq.yml"), 0o644)
	copy("deploy/observability/grafana/provisioning/dashboards/narumq.yml", filepath.Join(grafanaProvisioningDir, "dashboards", "narumq.yml"), 0o644)
	copy("deploy/observability/grafana/dashboards/narumq-operations.json", filepath.Join(grafanaDashboardsDir, "narumq", "narumq-operations.json"), 0o644)

	targetGroups := make([]dashboardStackTargetGroup, len(s.fixture.managementAddresses))
	for index, address := range s.fixture.managementAddresses {
		targetGroups[index] = dashboardStackTargetGroup{
			Targets: []string{"host.docker.internal:" + clusterMembershipProcessPort(address)},
			Labels: map[string]string{
				"cluster_id": clusterMembershipProcessClusterID,
				"node_id":    strconv.Itoa(index + 1),
			},
		}
	}
	targetData, err := json.MarshalIndent(targetGroups, "", "  ")
	if err != nil {
		s.t.Fatalf("encode Prometheus dynamic targets: %v", err)
	}
	if err := os.WriteFile(filepath.Join(prometheusTargetsDir, "narumq-cluster.json"), targetData, 0o644); err != nil {
		s.t.Fatalf("write Prometheus dynamic targets: %v", err)
	}
	readOnlyKey, err := os.ReadFile(s.fixture.readOnlyAPIKeyPath)
	if err != nil {
		s.t.Fatalf("read fixture ReadOnly API key: %v", err)
	}
	s.readOnlyAPIKey = string(readOnlyKey)
	if err := os.WriteFile(filepath.Join(prometheusSecretsDir, "narumq-readonly.key"), readOnlyKey, 0o644); err != nil {
		s.t.Fatalf("stage Prometheus ReadOnly API key: %v", err)
	}

	s.docker("network", "create", s.network)
	promPort := strings.TrimPrefix(s.prometheusURL, "http://127.0.0.1:")
	grafanaPort := strings.TrimPrefix(s.grafanaURL, "http://127.0.0.1:")
	s.docker("create", "--name", s.prometheus, "--network", s.network, "--network-alias", "prometheus", "--add-host", "host.docker.internal:host-gateway", "-p", "127.0.0.1:"+promPort+":9090", "prom/prometheus:v3.5.0", "--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus")
	s.docker("create", "--name", s.grafana, "--network", s.network, "--add-host", "host.docker.internal:host-gateway", "-p", "127.0.0.1:"+grafanaPort+":3000", "-e", "GF_SECURITY_ADMIN_USER=admin", "-e", "GF_SECURITY_ADMIN_PASSWORD="+s.adminPassword, "-e", "GF_PLUGINS_PREINSTALL_SYNC=yesoreyeram-infinity-datasource@4.1.1", "-e", "NARUMQ_MANAGEMENT_URL=http://host.docker.internal:"+clusterMembershipProcessPort(s.fixture.managementAddresses[0]), "-e", "NARUMQ_READONLY_API_KEY="+s.readOnlyAPIKey, "grafana/grafana-oss:13.0.2")
	s.docker("cp", prometheusDir, s.prometheus+":/etc")
	s.docker("cp", grafanaProvisioningDir, s.grafana+":/etc/grafana")
	s.docker("cp", grafanaDashboardsDir, s.grafana+":/var/lib/grafana")
	s.docker("start", s.prometheus)
	s.docker("start", s.grafana)
}

func (s *dashboardDockerStack) docker(args ...string) []byte {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.fixture.ctx, 2*time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		s.t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output
}

func (s *dashboardDockerStack) cleanup() {
	for _, name := range []string{s.grafana, s.prometheus} {
		if name != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_ = exec.CommandContext(ctx, "docker", "rm", "--force", name).Run()
			cancel()
		}
	}
	if s.network != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		_ = exec.CommandContext(ctx, "docker", "network", "rm", s.network).Run()
		cancel()
	}
}

func (s *dashboardDockerStack) request(method, endpoint string, body []byte, basicAuth bool) (int, []byte, error) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.fixture.ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	if len(body) != 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	if basicAuth {
		request.SetBasicAuth("admin", s.adminPassword)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	return response.StatusCode, data, err
}

func (s *dashboardDockerStack) waitForPrometheusTargets() {
	s.t.Helper()
	var lastStatus int
	var lastBody []byte
	var lastErr error
	for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline); {
		lastStatus, lastBody, lastErr = s.request(http.MethodGet, s.prometheusURL+"/api/v1/targets?state=active", nil, false)
		if lastErr == nil && lastStatus == http.StatusOK {
			var response struct {
				Data struct {
					ActiveTargets []struct {
						Labels    map[string]string `json:"labels"`
						Health    string            `json:"health"`
						LastError string            `json:"lastError"`
					} `json:"activeTargets"`
				} `json:"data"`
			}
			if json.Unmarshal(lastBody, &response) == nil {
				upNodes := make(map[string]bool)
				for _, target := range response.Data.ActiveTargets {
					if target.Labels["job"] == "narumq-management" && target.Health == "up" {
						upNodes[target.Labels["node_id"]] = true
					}
				}
				if len(upNodes) == 3 {
					return
				}
				lastErr = fmt.Errorf("only %d of 3 NaruMQ Prometheus targets are up", len(upNodes))
			}
		}
		select {
		case <-s.fixture.ctx.Done():
			s.t.Fatal(s.fixture.ctx.Err())
		case <-time.After(time.Second):
		}
	}
	logs := s.docker("logs", s.prometheus)
	s.t.Fatalf("Prometheus did not scrape all three nodes: status=%d body=%s err=%v\n%s", lastStatus, lastBody, lastErr, logs)
}

func (s *dashboardDockerStack) assertPrometheusDrainSeries() {
	s.t.Helper()
	endpoint := s.prometheusURL + "/api/v1/query?query=" + url.QueryEscape("narumq_node_drain_active")
	status, body, err := s.request(http.MethodGet, endpoint, nil, false)
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Prometheus drain metric query status=%d body=%s err=%v", status, body, err)
	}
	var response struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		s.t.Fatalf("decode Prometheus drain metric query: %v", err)
	}
	nodeIDs := make(map[string]bool)
	for _, result := range response.Data.Result {
		nodeIDs[result.Metric["node_id"]] = true
	}
	if len(nodeIDs) != 3 {
		s.t.Fatalf("Prometheus drain series node IDs = %v, want nodes 1, 2, and 3", nodeIDs)
	}
	for _, nodeID := range []string{"1", "2", "3"} {
		if !nodeIDs[nodeID] {
			s.t.Errorf("Prometheus drain series lacks node_id=%s", nodeID)
		}
	}
}

func (s *dashboardDockerStack) waitForGrafanaDashboard() {
	s.t.Helper()
	var lastStatus int
	var lastBody []byte
	var lastErr error
	endpoint := s.grafanaURL + "/api/dashboards/uid/narumq-operations"
	for deadline := time.Now().Add(4 * time.Minute); time.Now().Before(deadline); {
		running := strings.TrimSpace(string(s.docker("inspect", "--format", "{{.State.Running}}", s.grafana)))
		if running == "false" {
			logs := s.docker("logs", s.grafana)
			s.t.Fatalf("Grafana exited before provisioning the dashboard:\n%s", logs)
		}
		lastStatus, lastBody, lastErr = s.request(http.MethodGet, endpoint, nil, true)
		if lastErr == nil && lastStatus == http.StatusOK {
			return
		}
		select {
		case <-s.fixture.ctx.Done():
			s.t.Fatal(s.fixture.ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	logs := s.docker("logs", s.grafana)
	s.t.Fatalf("Grafana did not provision the dashboard: status=%d body=%s err=%v\n%s", lastStatus, lastBody, lastErr, logs)
}

func (s *dashboardDockerStack) assertGrafanaDatasourceHealth(uid string) {
	s.t.Helper()
	endpoint := s.grafanaURL + "/api/datasources/uid/" + url.PathEscape(uid) + "/health"
	status, body, err := s.request(http.MethodGet, endpoint, nil, true)
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Grafana datasource %s health status=%d body=%s err=%v", uid, status, body, err)
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &result); err != nil || result.Status != "OK" {
		s.t.Fatalf("Grafana datasource %s health response=%s decode=%v, want OK", uid, body, err)
	}
}
func (s *dashboardDockerStack) assertGrafanaDatasourceSet() {
	s.t.Helper()
	status, body, err := s.request(http.MethodGet, s.grafanaURL+"/api/datasources", nil, true)
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Grafana datasource list status=%d body=%s err=%v", status, body, err)
	}
	var datasources []struct {
		UID  string `json:"uid"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &datasources); err != nil {
		s.t.Fatalf("decode Grafana datasource list: %v body=%s", err, body)
	}
	got := make(map[string]string, len(datasources))
	for _, datasource := range datasources {
		got[datasource.UID] = datasource.Type
	}
	want := map[string]string{
		"narumq-prometheus": "prometheus",
		"narumq-management": "yesoreyeram-infinity-datasource",
	}
	if len(got) != len(want) {
		s.t.Fatalf("Grafana datasources = %v, want exactly %v", got, want)
	}
	for uid, datasourceType := range want {
		if got[uid] != datasourceType {
			s.t.Errorf("Grafana datasource %s type=%q, want %q; all=%v", uid, got[uid], datasourceType, got)
		}
	}
}

func (s *dashboardDockerStack) dashboardDefinition() dashboardStackDefinition {
	s.t.Helper()
	status, body, err := s.request(http.MethodGet, s.grafanaURL+"/api/dashboards/uid/narumq-operations", nil, true)
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Grafana dashboard lookup status=%d body=%s err=%v", status, body, err)
		return dashboardStackDefinition{}
	}
	var response struct {
		Dashboard dashboardStackDefinition `json:"dashboard"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		s.t.Fatalf("decode provisioned Grafana dashboard: %v", err)
		return dashboardStackDefinition{}
	}
	return response.Dashboard
}

func (s *dashboardDockerStack) dashboardPanel(panelID int) dashboardStackPanel {
	s.t.Helper()
	for _, panel := range s.dashboardDefinition().Panels {
		if panel.ID == panelID {
			return panel
		}
	}
	s.t.Fatalf("provisioned Grafana dashboard lacks panel %d", panelID)
	return dashboardStackPanel{}
}

func (s *dashboardDockerStack) queryGrafanaPanel(panelID int, nodeSelector, rateInterval, drainNode string) dashboardStackQueryResponse {
	s.t.Helper()
	panel := s.dashboardPanel(panelID)
	queries := make([]map[string]any, 0, len(panel.Targets))
	prometheus := false
	for _, target := range panel.Targets {
		query := map[string]any{
			"datasource":    map[string]string{"type": target.Datasource.Type, "uid": target.Datasource.UID},
			"refId":         target.RefID,
			"maxDataPoints": 1000,
			"intervalMs":    15000,
		}
		switch target.Datasource.UID {
		case "narumq-prometheus":
			prometheus = true
			expression := strings.ReplaceAll(target.Expr, "$node", nodeSelector)
			expression = strings.ReplaceAll(expression, "$__rate_interval", rateInterval)
			expression = strings.ReplaceAll(expression, "$rebalance_max_deviation", "100")
			query["editorMode"] = target.EditorMode
			query["expr"] = expression
			query["format"] = target.Format
			query["instant"] = target.Instant
			query["legendFormat"] = target.LegendFormat
			query["range"] = target.Range
		case "narumq-management":
			query["type"] = target.Type
			query["source"] = target.Source
			query["url"] = strings.ReplaceAll(target.URL, "${drain_node}", drainNode)
			query["url_options"] = map[string]string{"method": target.URLOptions.Method}
			query["parser"] = target.Parser
			query["format"] = target.Format
			query["root_selector"] = target.RootSelector
			query["columns"] = target.Columns
		default:
			s.t.Fatalf("panel %d uses unexpected datasource %q", panelID, target.Datasource.UID)
			return dashboardStackQueryResponse{}
		}
		queries = append(queries, query)
	}
	requestBody, err := json.Marshal(map[string]any{
		"from":     strconv.FormatInt(time.Now().Add(-5*time.Minute).UnixMilli(), 10),
		"to":       strconv.FormatInt(time.Now().UnixMilli(), 10),
		"timezone": "utc",
		"queries":  queries,
	})
	if err != nil {
		s.t.Fatalf("encode Grafana panel %d queries: %v", panelID, err)
		return dashboardStackQueryResponse{}
	}
	endpoint := s.grafanaURL + "/api/ds/query"
	if prometheus {
		endpoint += "?ds_type=prometheus"
	}
	status, body, err := s.request(http.MethodPost, endpoint, requestBody, true)
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Grafana panel %d query status=%d body=%s err=%v", panelID, status, body, err)
		return dashboardStackQueryResponse{}
	}
	var response dashboardStackQueryResponse
	if err := json.Unmarshal(body, &response); err != nil {
		s.t.Fatalf("decode Grafana panel %d query response: %v body=%s", panelID, err, body)
		return dashboardStackQueryResponse{}
	}
	for _, target := range panel.Targets {
		result, ok := response.Results[target.RefID]
		if !ok || result.Status != http.StatusOK || result.Error != "" {
			s.t.Fatalf("Grafana panel %d query %s result=%+v body=%s", panelID, target.RefID, result, body)
		}
	}
	return response
}

func dashboardStackFrameColumnValues(frame dashboardStackQueryFrame, fieldName string) []any {
	for index, field := range frame.Schema.Fields {
		if field.Name == fieldName && index < len(frame.Data.Values) {
			return frame.Data.Values[index]
		}
	}
	return nil
}

func dashboardStackResultColumnValues(result dashboardStackQueryResult, fieldName string) []any {
	for _, frame := range result.Frames {
		if values := dashboardStackFrameColumnValues(frame, fieldName); values != nil {
			return values
		}
	}
	return nil
}

func dashboardStackNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case int:
		return float64(number), true
	case int64:
		return float64(number), true
	case json.Number:
		result, err := number.Float64()
		return result, err == nil
	case string:
		result, err := strconv.ParseFloat(number, 64)
		return result, err == nil
	default:
		return 0, false
	}
}

func dashboardStackResultMaximum(result dashboardStackQueryResult) (float64, bool) {
	var maximum float64
	found := false
	for _, frame := range result.Frames {
		for fieldIndex, field := range frame.Schema.Fields {
			if field.Type != "number" || fieldIndex >= len(frame.Data.Values) {
				continue
			}
			for _, value := range frame.Data.Values[fieldIndex] {
				number, ok := dashboardStackNumber(value)
				if ok && (!found || number > maximum) {
					maximum = number
					found = true
				}
			}
		}
	}
	return maximum, found
}

func dashboardStackResultHasPositiveLabel(result dashboardStackQueryResult, labelName, labelValue string) bool {
	for _, frame := range result.Frames {
		for fieldIndex, field := range frame.Schema.Fields {
			if field.Labels[labelName] != labelValue || fieldIndex >= len(frame.Data.Values) {
				continue
			}
			for _, value := range frame.Data.Values[fieldIndex] {
				if number, ok := dashboardStackNumber(value); ok && number > 0 {
					return true
				}
			}
		}
	}
	return false
}
func dashboardStackResultHasPositiveLabelPair(
	result dashboardStackQueryResult,
	firstName, firstValue, secondName, secondValue string,
) bool {
	for _, frame := range result.Frames {
		for fieldIndex, field := range frame.Schema.Fields {
			if field.Labels[firstName] != firstValue ||
				field.Labels[secondName] != secondValue ||
				fieldIndex >= len(frame.Data.Values) {
				continue
			}
			for _, value := range frame.Data.Values[fieldIndex] {
				if number, ok := dashboardStackNumber(value); ok && number > 0 {
					return true
				}
			}
		}
	}
	return false
}

func dashboardStackResultFieldNumber(result dashboardStackQueryResult, fieldName string) (float64, bool) {
	values := dashboardStackResultColumnValues(result, fieldName)
	for index := len(values) - 1; index >= 0; index-- {
		if number, ok := dashboardStackNumber(values[index]); ok {
			return number, true
		}
	}
	return 0, false
}

func dashboardStackResultMetricValue(result dashboardStackQueryResult, metric, nodeID string) (float64, bool) {
	for _, frame := range result.Frames {
		for fieldIndex, field := range frame.Schema.Fields {
			if field.Labels["metric"] != metric || field.Labels["node_id"] != nodeID || fieldIndex >= len(frame.Data.Values) {
				continue
			}
			values := frame.Data.Values[fieldIndex]
			for index := len(values) - 1; index >= 0; index-- {
				if number, ok := dashboardStackNumber(values[index]); ok {
					return number, true
				}
			}
		}
	}
	return 0, false
}

func (s *dashboardDockerStack) assertGrafanaClusterOverview(process *clusterMembershipProcess) {
	s.t.Helper()
	quorum := s.queryGrafanaPanel(1, ".*", "1m", "")
	quorumValues := dashboardStackResultColumnValues(quorum.Results["A"], "Quorum")
	if len(quorumValues) == 0 || fmt.Sprint(quorumValues[len(quorumValues)-1]) != "true" {
		s.t.Fatalf("Grafana panel 1 quorum value=%v, want true", quorumValues)
	}

	assertNumber := func(panelID int, result dashboardStackQueryResult, fieldName string, want float64) {
		s.t.Helper()
		got, ok := dashboardStackResultFieldNumber(result, fieldName)
		if !ok || got != want {
			s.t.Errorf("Grafana panel %d field %q=%v (present=%t), want %v", panelID, fieldName, got, ok, want)
		}
	}
	replicas := s.queryGrafanaPanel(2, ".*", "1m", "")
	assertNumber(2, replicas.Results["A"], "Replica 부족", 0)
	members := s.queryGrafanaPanel(4, ".*", "1m", "")
	for _, expected := range []struct {
		field string
		value float64
	}{
		{field: "Members", value: 3},
		{field: "Quorum size", value: 2},
		{field: "Replication factor", value: 3},
		{field: "Write quorum", value: 2},
	} {
		assertNumber(4, members.Results["A"], expected.field, expected.value)
	}
	nodeStates := s.queryGrafanaPanel(5, ".*", "1m", "")
	for _, expected := range []struct {
		field string
		value float64
	}{
		{field: "Ready", value: 1},
		{field: "Joining", value: 2},
		{field: "Draining", value: 0},
		{field: "Safe to stop", value: 0},
	} {
		assertNumber(5, nodeStates.Results["A"], expected.field, expected.value)
	}

	status, body, err := s.managementRequest(process, http.MethodGet, "/api/v1/cluster")
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Management cluster overview status=%d body=%s err=%v", status, body, err)
	}
	var response struct {
		Data struct {
			UnderReplicated *int `json:"underReplicated"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil ||
		response.Data.UnderReplicated == nil || *response.Data.UnderReplicated != 0 {
		s.t.Fatalf("cluster underReplicated=%v decode=%v body=%s, want 0",
			response.Data.UnderReplicated, err, body)
	}
}

func (s *dashboardDockerStack) assertGrafanaNodeTopology(process *clusterMembershipProcess) {
	s.t.Helper()
	status, body, err := s.managementRequest(process, http.MethodGet, "/api/v1/nodes")
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Management node topology status=%d body=%s err=%v", status, body, err)
	}
	var response struct {
		Data []struct {
			NodeID          string  `json:"nodeId"`
			Role            string  `json:"role"`
			Version         string  `json:"version"`
			UptimeMillis    int64   `json:"uptimeMillis"`
			State           string  `json:"state"`
			Ready           bool    `json:"ready"`
			ReadinessReason *string `json:"readinessReason"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		s.t.Fatalf("decode Management node topology: %v body=%s", err, body)
	}
	if len(response.Data) != 3 {
		s.t.Fatalf("Management node topology has %d rows, want 3: %s", len(response.Data), body)
	}
	seen := make(map[string]bool, len(response.Data))
	leaderCount, followerCount := 0, 0
	for _, node := range response.Data {
		seen[node.NodeID] = true
		if node.Version == "" {
			s.t.Errorf("Management node %s has empty version", node.NodeID)
		}
		switch node.Role {
		case "LEADER":
			leaderCount++
		case "FOLLOWER":
			followerCount++
		default:
			s.t.Errorf("Management node %s role=%q, want LEADER or FOLLOWER", node.NodeID, node.Role)
		}
		if node.NodeID == "1" {
			if node.State != "READY" || !node.Ready || node.UptimeMillis <= 0 {
				s.t.Errorf("local Management node state=%q ready=%t uptime=%d, want READY/true/positive",
					node.State, node.Ready, node.UptimeMillis)
			}
			continue
		}
		if node.State != "JOINING" || node.ReadinessReason == nil ||
			*node.ReadinessReason != "remote readiness is not available from this node" {
			s.t.Errorf("remote Management node %s state=%q reason=%v, want JOINING with remote-readiness-unavailable reason",
				node.NodeID, node.State, node.ReadinessReason)
		}
	}
	if len(seen) != 3 || !seen["1"] || !seen["2"] || !seen["3"] || leaderCount != 1 || followerCount != 2 {
		s.t.Fatalf("Management nodes=%v roles leader=%d follower=%d, want nodes 1,2,3 and one leader/two followers",
			seen, leaderCount, followerCount)
	}
	s.assertGrafanaNodeTopologyPanel()
}
func (s *dashboardDockerStack) assertGrafanaNodeTopologyPanel() {
	s.t.Helper()
	result := s.queryGrafanaPanel(6, ".*", "1m", "").Results["A"]
	columnNames := []string{"Node", "Role", "Version", "Uptime (ms)", "State", "Ready", "Readiness reason"}
	columns := make(map[string][]any, len(columnNames))
	var nodeIDs []any
	for _, name := range columnNames {
		values := dashboardStackResultColumnValues(result, name)
		if len(values) != 3 {
			s.t.Fatalf("Grafana panel 6 column %q has %d values, want 3: %+v", name, len(values), result)
		}
		columns[name] = values
		if name == "Node" {
			nodeIDs = values
		}
	}
	seen := make(map[string]bool, len(nodeIDs))
	for index, value := range nodeIDs {
		nodeID := fmt.Sprint(value)
		seen[nodeID] = true
		if fmt.Sprint(columns["Version"][index]) == "" || fmt.Sprint(columns["Role"][index]) == "" {
			s.t.Errorf("Grafana panel 6 node %s lacks version or role", nodeID)
		}
		switch nodeID {
		case "1":
			uptime, ok := dashboardStackNumber(columns["Uptime (ms)"][index])
			if fmt.Sprint(columns["State"][index]) != "READY" ||
				fmt.Sprint(columns["Ready"][index]) != "true" || !ok || uptime <= 0 {
				s.t.Errorf("Grafana panel 6 node 1 state=%v ready=%v uptime=%v, want READY/true/positive",
					columns["State"][index], columns["Ready"][index], columns["Uptime (ms)"][index])
			}
		case "2", "3":
			if fmt.Sprint(columns["State"][index]) != "JOINING" ||
				fmt.Sprint(columns["Ready"][index]) != "false" ||
				fmt.Sprint(columns["Readiness reason"][index]) != "remote readiness is not available from this node" {
				s.t.Errorf("Grafana panel 6 node %s state=%v ready=%v reason=%v, want JOINING/false/remote-readiness-unavailable",
					nodeID, columns["State"][index], columns["Ready"][index], columns["Readiness reason"][index])
			}
		default:
			s.t.Errorf("Grafana panel 6 returned unexpected node %q", nodeID)
		}
	}
	if len(seen) != 3 || !seen["1"] || !seen["2"] || !seen["3"] {
		s.t.Errorf("Grafana panel 6 node IDs=%v, want 1,2,3", seen)
	}
}

func (s *dashboardDockerStack) assertGrafanaTrafficQueries() {
	s.t.Helper()
	connections := s.queryGrafanaPanel(7, ".*", "1m", "")
	count, ok := dashboardStackResultMaximum(connections.Results["A"])
	if !ok || count < 3 {
		s.t.Fatalf("Grafana panel 7 active connections=%v (present=%t), want at least 3", count, ok)
	}
	publish := s.queryGrafanaPanel(8, ".*", "1m", "")
	publishMaximum, hasPublishRate := dashboardStackResultMaximum(publish.Results["A"])
	if !hasPublishRate || publishMaximum <= 0 {
		s.t.Fatalf("Grafana panel 8 received PUBLISH rate=%v (present=%t), want a positive sample",
			publishMaximum, hasPublishRate)
	}
	protocols := s.queryGrafanaPanel(9, ".*", "1m", "")
	for _, protocol := range []string{"MQTT_3_1_1", "MQTT_5_0"} {
		if !dashboardStackResultHasPositiveLabel(protocols.Results["A"], "protocol", protocol) {
			s.t.Errorf("Grafana panel 9 lacks positive CONNECT success for protocol=%s", protocol)
		}
	}
	rejections := s.queryGrafanaPanel(10, ".*", "1m", "")
	if !dashboardStackResultHasPositiveLabelPair(
		rejections.Results["A"], "protocol", "MQTT_5_0", "reason", "not_authorized",
	) {
		s.t.Fatal("Grafana panel 10 lacks a positive MQTT_5_0/not_authorized CONNECT rejection series")
	}

	nodeOneConnections := s.queryGrafanaPanel(7, "1", "1m", "")
	nodeOneConnectionCount, hasNodeOneConnections := dashboardStackResultMaximum(nodeOneConnections.Results["A"])
	if !hasNodeOneConnections || nodeOneConnectionCount < 1 {
		s.t.Errorf("Grafana panel 7 node 1 connections=%v (present=%t), want at least one",
			nodeOneConnectionCount, hasNodeOneConnections)
	}
	nodeOnePublish := s.queryGrafanaPanel(8, "1", "1m", "")
	nodeOneReceiveRate, hasNodeOneReceiveRate := dashboardStackResultMaximum(nodeOnePublish.Results["A"])
	if !hasNodeOneReceiveRate || nodeOneReceiveRate <= 0 {
		s.t.Errorf("Grafana panel 8 node 1 received PUBLISH rate=%v (present=%t), want positive",
			nodeOneReceiveRate, hasNodeOneReceiveRate)
	}
	nodeTwoPublish := s.queryGrafanaPanel(8, "2", "1m", "")
	nodeTwoSendRate, hasNodeTwoSendRate := dashboardStackResultMaximum(nodeTwoPublish.Results["B"])
	if !hasNodeTwoSendRate || nodeTwoSendRate <= 0 {
		s.t.Errorf("Grafana panel 8 node 2 sent PUBLISH rate=%v (present=%t), want positive",
			nodeTwoSendRate, hasNodeTwoSendRate)
	}

	nodeOneProtocols := s.queryGrafanaPanel(9, "1", "1m", "")
	if !dashboardStackResultHasPositiveLabel(nodeOneProtocols.Results["A"], "protocol", "MQTT_3_1_1") ||
		dashboardStackResultHasPositiveLabel(nodeOneProtocols.Results["A"], "protocol", "MQTT_5_0") {
		s.t.Errorf("Grafana panel 9 node 1 must show MQTT_3_1_1 success without MQTT_5_0")
	}
	nodeTwoProtocols := s.queryGrafanaPanel(9, "2", "1m", "")
	if !dashboardStackResultHasPositiveLabel(nodeTwoProtocols.Results["A"], "protocol", "MQTT_5_0") ||
		dashboardStackResultHasPositiveLabel(nodeTwoProtocols.Results["A"], "protocol", "MQTT_3_1_1") {
		s.t.Errorf("Grafana panel 9 node 2 must show MQTT_5_0 success without MQTT_3_1_1")
	}
	nodeOneRejections := s.queryGrafanaPanel(10, "1", "1m", "")
	if dashboardStackResultHasPositiveLabelPair(
		nodeOneRejections.Results["A"], "protocol", "MQTT_5_0", "reason", "not_authorized",
	) {
		s.t.Errorf("Grafana panel 10 node 1 unexpectedly includes node 2 not_authorized rejection")
	}
	nodeTwoRejections := s.queryGrafanaPanel(10, "2", "1m", "")
	if !dashboardStackResultHasPositiveLabelPair(
		nodeTwoRejections.Results["A"], "protocol", "MQTT_5_0", "reason", "not_authorized",
	) {
		s.t.Errorf("Grafana panel 10 node 2 lacks MQTT_5_0/not_authorized rejection")
	}
}

func (s *dashboardDockerStack) assertGrafanaDashboardHas15Panels() {
	s.t.Helper()
	status, body, err := s.request(http.MethodGet, s.grafanaURL+"/api/dashboards/uid/narumq-operations", nil, true)
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("Grafana dashboard lookup status=%d body=%s err=%v", status, body, err)
	}
	var response struct {
		Dashboard dashboardStackDefinition `json:"dashboard"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		s.t.Fatalf("decode provisioned Grafana dashboard: %v", err)
	}
	dataPanels := 0
	for _, panel := range response.Dashboard.Panels {
		if panel.Type != "row" {
			dataPanels++
		}
	}
	if dataPanels != 15 {
		s.t.Fatalf("provisioned Grafana dashboard has %d data panels, want 15", dataPanels)
	}
}
func (s *dashboardDockerStack) assertGrafanaLoadAndDrainQueries() {
	s.t.Helper()
	response := s.queryGrafanaPanel(12, ".*", "1m", "")
	result := response.Results["A"]
	for _, metric := range []string{"connections_active", "deviation", "rebalance_needed"} {
		for _, nodeID := range []string{"1", "2", "3"} {
			if _, ok := dashboardStackResultMetricValue(result, metric, nodeID); !ok {
				s.t.Errorf("Grafana panel 12 lacks metric=%s for node_id=%s", metric, nodeID)
			}
		}
	}
	nodeThreeConnections, hasNodeThreeConnection := dashboardStackResultMetricValue(result, "connections_active", "3")
	if !hasNodeThreeConnection || nodeThreeConnections < 1 {
		s.t.Fatalf("Grafana panel 12 node 3 active connections=%v (present=%t), want at least one",
			nodeThreeConnections, hasNodeThreeConnection)
	}
	nodeOne := s.queryGrafanaPanel(12, "1", "1m", "")
	scopedResult := nodeOne.Results["A"]
	seenNodeOne := false
	for _, frame := range scopedResult.Frames {
		for _, field := range frame.Schema.Fields {
			if field.Labels["metric"] == "" {
				continue
			}
			if field.Labels["node_id"] != "1" {
				s.t.Errorf("Grafana panel 12 node=1 query returned node_id=%q", field.Labels["node_id"])
			}
			seenNodeOne = seenNodeOne || field.Labels["node_id"] == "1"
		}
	}
	if !seenNodeOne {
		s.t.Fatal("Grafana panel 12 node=1 query returned no node 1 metrics")
	}
}

func (s *dashboardDockerStack) assertGrafanaDrainPanels() {
	s.t.Helper()
	progress := s.queryGrafanaPanel(14, ".*", "1m", "3")
	result := progress.Results["A"]
	for _, metric := range []string{"drain_active", "connections_initial", "connections_remaining"} {
		for _, nodeID := range []string{"1", "2", "3"} {
			if _, ok := dashboardStackResultMetricValue(result, metric, nodeID); !ok {
				s.t.Errorf("Grafana panel 14 lacks metric=%s for node_id=%s", metric, nodeID)
			}
		}
	}
	initial, hasInitial := dashboardStackResultMetricValue(result, "connections_initial", "3")
	remaining, hasRemaining := dashboardStackResultMetricValue(result, "connections_remaining", "3")
	if !hasInitial || initial < 1 || !hasRemaining || remaining != 0 {
		s.t.Errorf("Grafana panel 14 node 3 initial=%v remaining=%v, want initial>=1 and remaining=0",
			initial, remaining)
	}
	detail := s.queryGrafanaPanel(15, ".*", "1m", "3").Results["A"]
	phase := dashboardStackResultColumnValues(detail, "Phase")
	state := dashboardStackResultColumnValues(detail, "State")
	safeToStop := dashboardStackResultColumnValues(detail, "Safe to stop")
	if len(phase) != 1 || fmt.Sprint(phase[0]) != "COMPLETED" ||
		len(state) != 1 || fmt.Sprint(state[0]) != "SAFE_TO_STOP" ||
		len(safeToStop) != 1 || fmt.Sprint(safeToStop[0]) != "true" {
		s.t.Errorf("Grafana panel 15 phase=%v state=%v safeToStop=%v, want COMPLETED/SAFE_TO_STOP/true",
			phase, state, safeToStop)
	}
	assertNumber := func(fieldName string, want float64) {
		s.t.Helper()
		got, ok := dashboardStackResultFieldNumber(detail, fieldName)
		if !ok || got != want {
			s.t.Errorf("Grafana panel 15 field %q=%v (present=%t), want %v", fieldName, got, ok, want)
		}
	}
	assertNumber("Connections remaining", 0)
	assertNumber("Sessions taken over", 1)
}

func (s *dashboardDockerStack) assertManagementReadOnlyKey(process *clusterMembershipProcess) {
	s.t.Helper()
	for _, check := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/api/v1/cluster", http.StatusOK},
		{http.MethodPost, "/api/v1/cluster/backups", http.StatusForbidden},
		{http.MethodPost, "/api/v1/nodes/2/drain", http.StatusForbidden},
	} {
		status, body, err := s.managementRequest(process, check.method, check.path)
		if err != nil || status != check.status {
			s.t.Fatalf("ReadOnly API key %s %s status=%d body=%s err=%v, want %d",
				check.method, check.path, status, body, err, check.status)
		}
	}
}

func (s *dashboardDockerStack) assertManagementDrainOperation(
	process *clusterMembershipProcess,
	expected clusterDrainProcessOperation,
) {
	s.t.Helper()
	status, body, err := s.managementRequest(process, http.MethodGet, "/api/v1/nodes/3/drain")
	if err != nil || status != http.StatusOK {
		s.t.Fatalf("ReadOnly drain detail status=%d body=%s err=%v", status, body, err)
	}
	actual, err := clusterDrainProcessDecodeOperation(body)
	if err != nil || actual.Phase != expected.Phase || actual.State != expected.State ||
		actual.SafeToStop != expected.SafeToStop ||
		actual.Connections.Initial != expected.Connections.Initial ||
		actual.Connections.Remaining != expected.Connections.Remaining {
		s.t.Fatalf("ReadOnly drain detail=%+v err=%v, want %+v; body=%s", actual, err, expected, body)
	}
}

func (s *dashboardDockerStack) managementRequest(
	process *clusterMembershipProcess,
	method, path string,
) (int, []byte, error) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.fixture.ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, process.managementURL()+path, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("create ReadOnly API key request: %w", err)
	}
	request.Header.Set("X-API-Key", s.readOnlyAPIKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, fmt.Errorf("send ReadOnly API key %s request: %w", method, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return response.StatusCode, body, fmt.Errorf("read ReadOnly API key response: %w", err)
	}
	return response.StatusCode, body, nil
}

type dashboardStackMQTTClients struct {
	subscriber net.Conn
	publisher  net.Conn
	drain      net.Conn
	probeV5    net.Conn
	probeV311  net.Conn
}

func (clients *dashboardStackMQTTClients) close() {
	for _, connection := range []net.Conn{clients.subscriber, clients.publisher, clients.drain, clients.probeV5, clients.probeV311} {
		if connection != nil {
			_ = connection.Close()
		}
	}
}

func dashboardStackGenerateTraffic(t *testing.T, fixture *clusterMembershipProcessFixture, processes []*clusterMembershipProcess) {
	t.Helper()
	clients := &dashboardStackMQTTClients{}
	t.Cleanup(clients.close)

	var reason byte
	var err error
	clients.subscriber, _, reason, err = clusterDrainProcessConnectV5(fixture.mqttAddresses[1], fixture.accessToken, "dashboard-sub-v5", true)
	if err != nil || reason != 0 {
		t.Fatalf("connect MQTT v5 dashboard subscriber: reason=0x%02x err=%v", reason, err)
	}
	clusterDrainProcessSubscribeV5(t, clients.subscriber, 1, "compat/dashboard", 0)
	clients.publisher = clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "dashboard-pub-v311", true)
	clients.drain, _, reason, err = clusterDrainProcessConnectV5(fixture.mqttAddresses[2], fixture.accessToken, "dashboard-drain-v5", false)
	if err != nil || reason != 0 {
		t.Fatalf("connect persistent MQTT v5 dashboard drain client: reason=0x%02x err=%v", reason, err)
	}
	clusterDrainProcessSubscribeV5(t, clients.drain, 2, "drain/dashboard", 0)
	clusterDrainProcessWaitForRouteProjection(t, fixture.ctx, processes, map[string]clusterDrainProcessSubscription{
		"dashboard-sub-v5":   {topicFilter: "compat/dashboard", qos: 0},
		"dashboard-drain-v5": {topicFilter: "drain/dashboard", qos: 0},
	})

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for index := 0; index < 60; index++ {
		if index == 40 {
			clients.probeV311 = clusterDurableProcessConnect(t, fixture.mqttAddresses[0], fixture.accessToken, "dashboard-probe-v311", true)
			clients.probeV5, _, reason, err = clusterDrainProcessConnectV5(fixture.mqttAddresses[1], fixture.accessToken, "dashboard-probe-v5", true)
			if err != nil || reason != 0 {
				t.Fatalf("connect MQTT v5 dashboard probe: reason=0x%02x err=%v", reason, err)
			}
			rejected, _, rejectedReason, rejectErr := clusterDrainProcessConnectV5(fixture.mqttAddresses[1], "invalid-dashboard-token", "dashboard-reject-v5", true)
			if rejected != nil {
				_ = rejected.Close()
			}
			if rejectErr != nil || rejectedReason != 0x87 {
				t.Fatalf("invalid MQTT v5 dashboard probe reason=0x%02x err=%v, want not_authorized (0x87)", rejectedReason, rejectErr)
			}
		}
		clusterDurableProcessPublish(t, clients.publisher, "compat/dashboard", 0, 0, fmt.Sprintf("dashboard message %d", index))
		_ = clients.subscriber.SetReadDeadline(time.Now().Add(5 * time.Second))
		kind, _, _, readErr := clusterDurableProcessReadRaw(clients.subscriber)
		_ = clients.subscriber.SetReadDeadline(time.Time{})
		if readErr != nil || kind != 3 {
			t.Fatalf("read MQTT v5 dashboard subscriber PUBLISH kind=%d err=%v", kind, readErr)
		}
		if index == 25 || index == 50 || index == 59 {
			for _, connection := range []net.Conn{clients.subscriber, clients.drain, clients.probeV311, clients.probeV5} {
				if connection != nil {
					clusterDrainProcessPing(t, connection)
				}
			}
		}
		if index != 59 {
			select {
			case <-fixture.ctx.Done():
				t.Fatalf("dashboard MQTT workload context ended: %v", fixture.ctx.Err())
			case <-ticker.C:
			}
		}
	}
	dashboardStackWait(t, fixture.ctx, 20*time.Second)
}

func dashboardStackWait(t *testing.T, ctx context.Context, duration time.Duration) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		t.Fatalf("dashboard stack wait context ended: %v", ctx.Err())
	case <-timer.C:
	}
}
