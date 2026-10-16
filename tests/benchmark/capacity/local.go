package main

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"narumq/tests/benchmark/internal/environment"
)

const (
	localBenchmarkID  = "capacity"
	localClusterID    = "narumq-capacity"
	localMQTTAlias    = "mqtt.capacity.test"
	localNodeCount    = 3
	localStopTimeout  = 10 * time.Second
	localStartTimeout = 120 * time.Second
	localMgmtPort     = "8080/tcp"
)

type clusterConfigDoc struct {
	Mode                 string            `json:"mode"`
	ClusterID            string            `json:"clusterId"`
	NodeID               uint64            `json:"nodeId"`
	Directory            string            `json:"directory"`
	ListenAddress        string            `json:"listenAddress"`
	AdvertisedAddress    string            `json:"advertisedAddress"`
	LocalDNSName         string            `json:"localDnsName"`
	ClusterProtocolMajor uint32            `json:"clusterProtocolMajor"`
	ClusterProtocolMinor uint32            `json:"clusterProtocolMinor"`
	StateSchemaMajor     uint32            `json:"stateSchemaMajor"`
	StateSchemaMinor     uint32            `json:"stateSchemaMinor"`
	Role                 string            `json:"role"`
	BootstrapMembers     []bootstrapMember `json:"bootstrapMembers"`
	SeedAddresses        []string          `json:"seedAddresses"`
	PeerTLS              peerTLSDoc        `json:"peerTLS"`
	Management           managementDoc     `json:"management"`
	MQTT                 mqttDoc           `json:"mqtt"`
}

type bootstrapMember struct {
	MemberID          uint64 `json:"memberId"`
	Address           string `json:"address"`
	DNSSAN            string `json:"dnsSan"`
	CertificateSHA256 string `json:"certificateSha256"`
}

type peerTLSDoc struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
	CAFile   string `json:"caFile"`
}

type managementDoc struct {
	ListenAddress         string `json:"listenAddress"`
	ClusterAdminTokenFile string `json:"clusterAdminTokenFile"`
	AuditDirectory        string `json:"auditDirectory"`
}

type mqttDoc struct {
	TCPListenAddress   string      `json:"tcpListenAddress"`
	TLSListenAddress   string      `json:"tlsListenAddress"`
	SecurityConfigFile string      `json:"securityConfigFile"`
	TLS                *mqttTLSDoc `json:"tls,omitempty"`
}

type mqttTLSDoc struct {
	CertFile   string `json:"certFile"`
	KeyFile    string `json:"keyFile"`
	CAFile     string `json:"caFile"` // required key; empty because client certificates are not requested
	ClientAuth string `json:"clientAuth"`
	MinVersion string `json:"minVersion"`
}

type localFixture struct {
	dir             string
	runnerDir       string
	networkOwnerDir string
	nodeDirs        []string
	managementToken string
}

func nodeDNS(number int) string { return fmt.Sprintf("node%d.capacity.test", number) }

func newLocalFixture(p Profile) (*localFixture, error) {
	dir, err := os.MkdirTemp("", "narumq-capacity-*")
	if err != nil {
		return nil, err
	}
	fixture := &localFixture{dir: dir, runnerDir: filepath.Join(dir, "runner"), networkOwnerDir: filepath.Join(dir, "network-owner")}
	if err := fixture.build(p); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return fixture, nil
}

func (f *localFixture) close() error { return os.RemoveAll(f.dir) }

func (f *localFixture) build(p Profile) error {
	for _, dir := range []string{f.dir, f.runnerDir, f.networkOwnerDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	adminBytes := make([]byte, 32)
	if _, err := rand.Read(adminBytes); err != nil {
		return err
	}
	f.managementToken = hex.EncodeToString(adminBytes)
	signer, err := newJWTSigner()
	if err != nil {
		return err
	}
	creds, err := signer.mintCredentials(p, fixtureTTL)
	if err != nil {
		return err
	}
	credsJSON, err := json.Marshal(creds)
	if err != nil {
		return err
	}
	ca, err := newFixtureCA(fixtureTTL)
	if err != nil {
		return err
	}
	securityJSON, err := json.Marshal(buildSecurityConfig(p, "http://127.0.0.1:18080"+jwksListenPath))
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{
		"credentials.json": credsJSON, "ca.pem": ca.pem(), "management.token": []byte(f.managementToken + "\n"),
	} {
		if err := writeSecretFile(filepath.Join(f.runnerDir, name), data); err != nil {
			return err
		}
	}
	if err := writeSecretFile(filepath.Join(f.networkOwnerDir, jwksFileName), signer.jwksJSON); err != nil {
		return err
	}

	peerCerts := make([]issuedCertificate, localNodeCount)
	members := make([]bootstrapMember, localNodeCount)
	for index := range peerCerts {
		number := index + 1
		peerCerts[index], err = ca.issue(fixtureTTL, []string{nodeDNS(number)}, x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth)
		if err != nil {
			return err
		}
		members[index] = bootstrapMember{
			MemberID: uint64(number), Address: nodeDNS(number) + ":7000", DNSSAN: nodeDNS(number),
			CertificateSHA256: peerCerts[index].fingerprint,
		}
	}
	f.nodeDirs = make([]string, localNodeCount)
	for index := range peerCerts {
		number := index + 1
		mqttCert, err := ca.issue(fixtureTTL, []string{localMQTTAlias, nodeDNS(number)}, x509.ExtKeyUsageServerAuth)
		if err != nil {
			return err
		}
		nodeDir := filepath.Join(f.dir, fmt.Sprintf("node-%d", number))
		if err := os.MkdirAll(nodeDir, 0o700); err != nil {
			return err
		}
		f.nodeDirs[index] = nodeDir
		config := clusterConfigDoc{
			Mode: "cluster", ClusterID: localClusterID, NodeID: uint64(number),
			Directory: "/var/lib/narumq/node", ListenAddress: "0.0.0.0:7000",
			AdvertisedAddress: nodeDNS(number) + ":7000", LocalDNSName: nodeDNS(number),
			ClusterProtocolMajor: 2, ClusterProtocolMinor: 0, StateSchemaMajor: 1, StateSchemaMinor: 0,
			Role: "bootstrap", BootstrapMembers: members, SeedAddresses: []string{},
			PeerTLS:    peerTLSDoc{CertFile: "/config/node.pem", KeyFile: "/config/node-key.pem", CAFile: "/config/ca.pem"},
			Management: managementDoc{ListenAddress: "0.0.0.0:8080", ClusterAdminTokenFile: "/config/admin.token", AuditDirectory: "/var/lib/narumq/audit"},
			MQTT: mqttDoc{
				TCPListenAddress: "0.0.0.0:1883", TLSListenAddress: "0.0.0.0:8883", SecurityConfigFile: "/config/mqtt-security.json",
				TLS: &mqttTLSDoc{CertFile: "/config/mqtt.pem", KeyFile: "/config/mqtt-key.pem", ClientAuth: "none", MinVersion: "1.2"},
			},
		}
		configJSON, err := json.Marshal(config)
		if err != nil {
			return err
		}
		for name, data := range map[string][]byte{
			"ca.pem": ca.pem(), "node.pem": peerCerts[index].certificatePEM, "node-key.pem": peerCerts[index].keyPEM,
			"mqtt.pem": mqttCert.certificatePEM, "mqtt-key.pem": mqttCert.keyPEM,
			"admin.token": []byte(f.managementToken + "\n"), "mqtt-security.json": securityJSON, "node.json": configJSON,
		} {
			if err := writeSecretFile(filepath.Join(nodeDir, name), data); err != nil {
				return err
			}
		}
	}
	return nil
}

// localTimeout bounds the whole local run by its worst-case number of step-runs.
func localTimeout(p Profile) time.Duration {
	runs := len(p.Steps) + p.BisectRounds + p.ConfirmRuns + 2
	var longest time.Duration
	for _, step := range p.Steps {
		longest = max(longest, stepTimeoutFor(p, step.Cells))
	}
	return time.Duration(runs)*longest + 10*time.Minute
}

func repositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("go.mod was not found above the working directory")
		}
		current = parent
	}
}

func localCommand(ctx context.Context, args []string) error {
	var profilePath, outputRoot string
	var keepImages bool
	flags := flag.NewFlagSet("local", flag.ContinueOnError)
	flags.StringVar(&profilePath, "profile", "", "capacity profile JSON")
	flags.StringVar(&outputRoot, "output", filepath.Join(".artifacts", "verification"), "artifact root directory")
	flags.BoolVar(&keepImages, "keep-images", false, "keep the built Docker images after the run")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if profilePath == "" {
		return errors.New("--profile is required")
	}
	p, profileBytes, _, err := LoadProfile(profilePath)
	if err != nil {
		return err
	}
	repoRoot, err := repositoryRoot()
	if err != nil {
		return err
	}
	runID := time.Now().UTC().Format("20060102T150405Z")
	runDir, err := filepath.Abs(filepath.Join(outputRoot, "capacity-local-"+runID))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return err
	}
	profileCopy := filepath.Join(runDir, "profile.json")
	if err := os.WriteFile(profileCopy, profileBytes, 0o644); err != nil {
		return err
	}
	images, err := environment.BuildImages(ctx, environment.BuildOptions{
		BenchmarkID: localBenchmarkID, RunID: runID, RepoRoot: repoRoot, ArtifactDir: runDir, RunnerTarget: "capacity-runner",
	})
	if err != nil {
		return fmt.Errorf("build images: %w", err)
	}
	if !keepImages {
		defer removeImages(images.NaruMQReference, images.RunnerReference)
	}
	fixture, err := newLocalFixture(p)
	if err != nil {
		return fmt.Errorf("create fixture: %w", err)
	}
	defer func() { _ = fixture.close() }()

	nodes := make([]environment.ClusterNodeSpec, localNodeCount)
	for index := range nodes {
		number := index + 1
		nodes[index] = environment.ClusterNodeSpec{
			Name: fmt.Sprintf("node%d", number), DNSName: nodeDNS(number), ConfigDirectory: fixture.nodeDirs[index],
			MountConfig: true, DataDestination: "/var/lib/narumq", BrokerArgs: []string{"-cluster-config", "/config/node.json"},
			PublishedPorts: []string{localMgmtPort},
		}
	}
	controller := &localController{profile: p, runDir: runDir, token: fixture.managementToken}
	options := environment.ClusterTargetOptions{
		BenchmarkID: localBenchmarkID, RunID: runID, Target: "narumq", ArtifactDirectory: runDir, ProfilePath: profileCopy,
		ImageReference: images.NaruMQReference, RunnerReference: images.RunnerReference, RunnerImageID: images.RunnerID,
		RunnerTarget:                images.RunnerTarget,
		NetworkOwnerArgs:            []string{"serve-jwks", "--file", "/config/jwks.json", "--listen", "127.0.0.1:18080"},
		NetworkOwnerConfigDirectory: fixture.networkOwnerDir, RunnerConfigDirectory: fixture.runnerDir,
		Nodes: nodes, ExtraNetworkAliases: []string{localMQTTAlias},
		BrokerCPUs: p.BrokerCPUs, BrokerMemoryBytes: p.BrokerMemoryBytes,
		GeneratorCPUs: p.GeneratorCPUs, GeneratorMemoryBytes: p.GeneratorMemoryBytes,
		RunnerArgs: []string{
			"run", "--profile", "/input/profile.json",
			"--mqtt", localMQTTAlias + ":1883", "--mqtt-tls", localMQTTAlias + ":8883",
			"--tls-ca", "/runner-config/ca.pem", "--tls-server-name", localMQTTAlias,
			"--management", "http://" + localMQTTAlias + ":8080", "--management-token-file", "/runner-config/management.token",
			"--credentials", "/runner-config/credentials.json", "--control", "/output/control", "--output", "/output",
		},
		Timeout: localTimeout(p), StartupTimeout: localStartTimeout,
		Controller: controller.run, AllowBrokerRestart: true,
	}
	result, runErr := environment.RunClusterTarget(ctx, options)
	if data, err := json.MarshalIndent(result, "", "  "); err == nil {
		_ = os.WriteFile(filepath.Join(runDir, "environment-result.json"), data, 0o644)
	}
	if runErr != nil {
		return runErr
	}
	if !result.Success {
		return fmt.Errorf("local capacity run failed: %s", result.FailureReason)
	}
	fmt.Println(filepath.Join(runDir, "capacity-summary.json"))
	return nil
}

func removeImages(references ...string) {
	args := append([]string{"image", "rm", "--force"}, references...)
	_ = exec.Command("docker", args...).Run()
}

// localController injects the leader SIGKILL and restore for each step-run through the
// control markers; the runner never touches Docker.
type localController struct {
	profile Profile
	runDir  string
	token   string
}

func (c *localController) controlDir() string { return filepath.Join(c.runDir, "control") }

func (c *localController) writeKill(step, run int, state string, target killTarget, cause error) error {
	marker := killMarker{State: state, Target: target, At: formatTime(time.Now())}
	if cause != nil {
		marker.Error = cause.Error()
	}
	return writeAtomicJSON(markerPath(c.controlDir(), step, run, "kill"), marker)
}

// nextReady finds a ready marker that has not been handled yet.
func (c *localController) nextReady(handled map[string]bool) (step, run int, ok bool) {
	entries, err := os.ReadDir(c.controlDir())
	if err != nil {
		return 0, 0, false
	}
	for _, entry := range entries {
		name := entry.Name()
		if handled[name] || !strings.HasSuffix(name, "-ready.json") {
			continue
		}
		var s, r int
		if _, err := fmt.Sscanf(name, "step-%d-r%d-ready.json", &s, &r); err == nil {
			handled[name] = true
			return s, r, true
		}
	}
	return 0, 0, false
}

func (c *localController) run(ctx context.Context, nodes []environment.ClusterNodeResult) error {
	handled := make(map[string]bool)
	for {
		if fileExists(filepath.Join(c.controlDir(), "run-complete.json")) {
			return nil
		}
		if step, run, ok := c.nextReady(handled); ok {
			if err := c.handle(ctx, nodes, step, run); err != nil {
				return err
			}
			continue
		}
		if !sleepCtx(ctx, 50*time.Millisecond) {
			return ctx.Err()
		}
	}
}

// handle performs one leader kill and restore. Injection failures are reported to the runner
// as a failed marker (an invalid step); only context cancellation stops the controller.
func (c *localController) handle(ctx context.Context, nodes []environment.ClusterNodeResult, step, run int) error {
	var ready readyMarker
	if err := readControlJSON(markerPath(c.controlDir(), step, run, "ready"), &ready); err != nil {
		return c.fail(step, run, killTarget{}, fmt.Errorf("read ready marker: %w", err))
	}
	number, err := strconv.Atoi(ready.LeaderNodeID)
	if err != nil || number < 1 || number > len(nodes) {
		return c.fail(step, run, killTarget{NodeID: ready.LeaderNodeID}, fmt.Errorf("leader node ID %q is not a local node", ready.LeaderNodeID))
	}
	victim := nodes[number-1]
	target := killTarget{NodeID: ready.LeaderNodeID, Container: victim.BrokerContainer}
	if err := c.writeKill(step, run, killStateKilling, target, nil); err != nil {
		return err
	}
	ackTimeout := time.Duration(c.profile.MarkerAckTimeoutMillis) * time.Millisecond
	if !waitForMarker(ctx, ackTimeout, func() bool { return fileExists(markerPath(c.controlDir(), step, run, "kill-ack")) }) {
		return c.fail(step, run, target, errors.New("runner did not acknowledge the killing marker"))
	}
	if err := environment.KillClusterBroker(ctx, victim.BrokerContainer, localStopTimeout); err != nil {
		return c.fail(step, run, target, fmt.Errorf("kill leader: %w", err))
	}
	if err := c.writeKill(step, run, killStateDown, target, nil); err != nil {
		return err
	}
	// The runner finishes measureN1 and stops publishing before the node returns.
	n1 := markerPath(c.controlDir(), step, run, "n1-done")
	for !fileExists(n1) {
		if !sleepCtx(ctx, markerPollInterval) {
			return ctx.Err()
		}
	}
	if err := c.writeKill(step, run, killStateRestoring, target, nil); err != nil {
		return err
	}
	if err := environment.StartClusterBroker(ctx, victim.BrokerContainer, localStartTimeout); err != nil {
		return c.fail(step, run, target, fmt.Errorf("restart leader: %w", err))
	}
	if err := c.waitResynced(ctx, nodes, number-1); err != nil {
		return c.fail(step, run, target, err)
	}
	return c.writeKill(step, run, killStateRestored, target, nil)
}

// waitResynced reads management state through a surviving node's published port.
func (c *localController) waitResynced(ctx context.Context, nodes []environment.ClusterNodeResult, victim int) error {
	address := nodes[(victim+1)%len(nodes)].PublishedPorts[localMgmtPort]
	if address == "" {
		return errors.New("management port of a surviving node is not published")
	}
	client := newMgmtClient("http://"+address, c.token, nil)
	timeout := time.Duration(c.profile.ReadyTimeoutMillis+c.profile.ResyncTimeoutMillis) * time.Millisecond
	if err := client.waitSettled(ctx, timeout); err != nil {
		return fmt.Errorf("restored leader did not resync: %w", err)
	}
	return nil
}

func (c *localController) fail(step, run int, target killTarget, cause error) error {
	if err := c.writeKill(step, run, killStateFailed, target, cause); err != nil {
		return err
	}
	return nil
}
