package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const collectedWaitLimit = time.Hour

type runOptions struct {
	profile, mqtt, mqttTLS, tlsCA, tlsServerName string
	management, managementTokenFile, credentials string
	control, output, metadata                    string
	awaitCollected                               bool
}

// runner drives one capacity run against an externally provided cluster. It knows nothing
// about Docker or Kubernetes: failure injection happens through the control markers.
type runner struct {
	ctx        context.Context
	opts       runOptions
	profile    Profile
	profileSHA string
	creds      map[string]credential
	endpoints  *endpoints
	mgmt       *mgmtClient
	controlDir string
	outDir     string
	nonce      uint32
	// features are the cluster features verified active by the latest step's readiness stage.
	features []string
}

func parseRunOptions(args []string) (runOptions, error) {
	var opts runOptions
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.StringVar(&opts.profile, "profile", "", "capacity profile JSON")
	flags.StringVar(&opts.mqtt, "mqtt", "", "plaintext MQTT host:port")
	flags.StringVar(&opts.mqttTLS, "mqtt-tls", "", "MQTT over TLS host:port")
	flags.StringVar(&opts.tlsCA, "tls-ca", "", "CA PEM file for the MQTT TLS endpoint")
	flags.StringVar(&opts.tlsServerName, "tls-server-name", "", "TLS server name")
	flags.StringVar(&opts.management, "management", "", "management API base URL")
	flags.StringVar(&opts.managementTokenFile, "management-token-file", "", "file holding the management bearer token")
	flags.StringVar(&opts.credentials, "credentials", "", "per-tenant credentials JSON")
	flags.StringVar(&opts.control, "control", "", "control marker directory")
	flags.StringVar(&opts.output, "output", "", "artifact output directory")
	flags.StringVar(&opts.metadata, "metadata", "", "optional controller metadata JSON merged into manifest.json")
	flags.BoolVar(&opts.awaitCollected, "await-collected", false, "wait for control/collected.json after run-complete.json")
	if err := flags.Parse(args); err != nil {
		return runOptions{}, err
	}
	if flags.NArg() != 0 {
		return runOptions{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	required := map[string]string{
		"--profile": opts.profile, "--mqtt": opts.mqtt, "--mqtt-tls": opts.mqttTLS, "--tls-ca": opts.tlsCA,
		"--tls-server-name": opts.tlsServerName, "--management": opts.management,
		"--management-token-file": opts.managementTokenFile, "--credentials": opts.credentials,
		"--control": opts.control, "--output": opts.output,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return runOptions{}, fmt.Errorf("%s is required", name)
		}
	}
	return opts, nil
}

func loadCredentials(path string, p Profile) (map[string]credential, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}
	var creds map[string]credential
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("decode credentials: %w", err)
	}
	for cell := range p.MaxCells() {
		for _, kind := range []byte{'a', 'b'} {
			tenant := tenantID(kind, cell)
			if creds[tenant].Token == "" || creds[tenant].Username == "" {
				return nil, fmt.Errorf("credentials for tenant %s are missing", tenant)
			}
		}
	}
	return creds, nil
}

func runCommand(ctx context.Context, args []string) error {
	opts, err := parseRunOptions(args)
	if err != nil {
		return err
	}
	p, _, sha, err := LoadProfile(opts.profile)
	if err != nil {
		return err
	}
	creds, err := loadCredentials(opts.credentials, p)
	if err != nil {
		return err
	}
	token, err := readSecretFile(opts.managementTokenFile)
	if err != nil {
		return fmt.Errorf("read management token: %w", err)
	}
	ends, err := newEndpoints(opts.mqtt, opts.mqttTLS, opts.tlsCA, opts.tlsServerName)
	if err != nil {
		return err
	}
	var nonce [4]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	r := &runner{
		ctx: ctx, opts: opts, profile: p, profileSHA: sha, creds: creds, endpoints: ends,
		mgmt: newMgmtClient(opts.management, token, nil), controlDir: opts.control, outDir: opts.output,
		nonce: uint32(binary.BigEndian.Uint16(nonce[:2])),
	}
	return r.execute()
}

func (r *runner) writeManifest(started time.Time) error {
	manifest := map[string]any{
		"schemaVersion": 1, "profileId": r.profile.ProfileID, "purpose": r.profile.Purpose, "profileSha256": r.profileSHA,
		"releaseGating": false, "startedAt": formatTime(started),
		"runner": map[string]any{"goVersion": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "cpus": runtime.NumCPU()},
		"endpoints": map[string]any{
			"mqtt": r.opts.mqtt, "mqttTls": r.opts.mqttTLS, "tlsServerName": r.opts.tlsServerName, "management": r.opts.management,
		},
		"profile": r.profile,
	}
	if r.opts.metadata != "" {
		data, err := os.ReadFile(r.opts.metadata)
		if err != nil {
			return fmt.Errorf("read metadata: %w", err)
		}
		var metadata map[string]any
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("decode metadata: %w", err)
		}
		manifest["controller"] = metadata
	}
	return writeAtomicJSON(filepath.Join(r.outDir, "manifest.json"), manifest)
}

func (r *runner) execute() error {
	if err := os.MkdirAll(r.outDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(r.controlDir, 0o755); err != nil {
		return err
	}
	started := time.Now()
	if err := r.writeManifest(started); err != nil {
		return err
	}
	outcome, driveErr := driveCapacity(r.profile, r.executeStep)
	if driveErr != nil {
		outcome.Aborted = driveErr.Error()
	}
	summary := buildCapacitySummary(r.profile, r.profileSHA, started, time.Now(), outcome)
	summary.ActiveClusterFeatures = r.features
	summaryErr := writeAtomicJSON(filepath.Join(r.outDir, "capacity-summary.json"), summary)
	markerErr := writeAtomicJSON(filepath.Join(r.controlDir, "run-complete.json"), runCompleteMarker{
		ProfileID: r.profile.ProfileID, Output: r.outDir, Adopted: summary.Adopted != nil, Aborted: summary.Aborted,
		WrittenAt: formatTime(time.Now()),
	})
	if err := errors.Join(driveErr, summaryErr, markerErr); err != nil {
		return err
	}
	fmt.Println(summaryLine(summary))
	if r.opts.awaitCollected {
		if !waitForMarker(r.ctx, collectedWaitLimit, func() bool { return fileExists(filepath.Join(r.controlDir, "collected.json")) }) {
			return errors.New("collected.json was not written before the wait limit")
		}
	}
	return nil
}
