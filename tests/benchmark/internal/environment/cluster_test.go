package environment

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	fakeDockerModeEnv                   = "NARUMQ_ENVIRONMENT_FAKE_DOCKER"
	fakeDockerStateEnv                  = "NARUMQ_ENVIRONMENT_FAKE_DOCKER_STATE"
	fakeDockerCallsEnv                  = "NARUMQ_ENVIRONMENT_FAKE_DOCKER_CALLS"
	fakeDockerReleaseEnv                = "NARUMQ_ENVIRONMENT_FAKE_DOCKER_RELEASE"
	fakeDockerEmptyPortEnv              = "NARUMQ_ENVIRONMENT_FAKE_DOCKER_EMPTY_PORT"
	fakeDockerStartAfterStoppedStatsEnv = "NARUMQ_ENVIRONMENT_FAKE_DOCKER_START_AFTER_STOPPED_STATS"
)

var fakeDockerBinary struct {
	sync.Once
	directory string
	data      []byte
	err       error
}

func TestMain(m *testing.M) {
	if os.Getenv(fakeDockerModeEnv) == "1" {
		os.Exit(runFakeDocker(os.Args[1:]))
	}
	code := m.Run()
	if fakeDockerBinary.directory != "" {
		if err := os.RemoveAll(fakeDockerBinary.directory); err != nil {
			fmt.Fprintf(os.Stderr, "remove fake Docker binary directory: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

func TestRunClusterTargetKeepsPublishedOwnerPortAcrossBrokerRestart(t *testing.T) {
	fakeDocker := installFakeDocker(t)
	options := validBP07ClusterOptions(t)
	options.Nodes[0].PublishedPorts = []string{"8080"}
	options.AllowBrokerRestart = true
	options.Controller = func(ctx context.Context, nodes []ClusterNodeResult) error {
		before := nodes[0].PublishedPorts["8080"]
		if before == "" {
			return errors.New("owner management port was not resolved before controller start")
		}
		if err := RestartClusterBroker(ctx, nodes[0].BrokerContainer, 10*time.Second, 3*time.Second); err != nil {
			return err
		}
		after, err := resolveClusterPublishedPorts(ctx, nodes[0].NetworkOwnerContainer, []string{"8080"})
		if err != nil {
			return err
		}
		if after["8080"] != before {
			return fmt.Errorf("owner port changed across broker restart: before=%q after=%q", before, after["8080"])
		}
		return os.WriteFile(fakeDocker.releasePath, []byte("done"), 0o600)
	}

	result, err := RunClusterTarget(context.Background(), options)
	if err != nil {
		t.Fatalf("RunClusterTarget() error = %v", err)
	}
	if !result.Success {
		t.Fatalf("RunClusterTarget() failed: %s", result.FailureReason)
	}
	if got := result.Nodes[0].PublishedPorts["8080"]; got != "127.0.0.1:49152" {
		t.Fatalf("owner published port = %q, want 127.0.0.1:49152", got)
	}

	calls := readFakeDockerCalls(t, fakeDocker.callsPath)
	ownerArgs := findNamedContainerRun(t, calls, result.Nodes[0].NetworkOwnerContainer)
	publishIndex := indexOf(ownerArgs, "--publish")
	imageIndex := indexOf(ownerArgs, options.RunnerReference)
	if publishIndex < 0 || publishIndex+1 >= imageIndex || ownerArgs[publishIndex+1] != "127.0.0.1::8080" {
		t.Fatalf("network owner args = %#v, want loopback publish before image", ownerArgs)
	}

	portCalls := make([]int, 0, 2)
	stopIndex, startIndex := -1, -1
	for index, call := range calls {
		if len(call) >= 3 && call[0] == "port" && call[1] == result.Nodes[0].NetworkOwnerContainer && call[2] == "8080" {
			portCalls = append(portCalls, index)
		}
		if len(call) >= 4 && call[0] == "stop" && call[1] == "--time" && call[2] == "10" && call[3] == result.Nodes[0].BrokerContainer && stopIndex < 0 {
			stopIndex = index
		}
		if len(call) >= 2 && call[0] == "start" && call[1] == result.Nodes[0].BrokerContainer && startIndex < 0 {
			startIndex = index
		}
	}
	restartReadyIndex := -1
	for index, call := range calls {
		if index > startIndex && len(call) == 4 && call[0] == "inspect" && call[1] == "--format" && call[2] == "{{.State.Status}}" && call[3] == result.Nodes[0].BrokerContainer {
			restartReadyIndex = index
			break
		}
	}
	if len(portCalls) != 2 || !(portCalls[0] < stopIndex && stopIndex < startIndex && startIndex < restartReadyIndex && restartReadyIndex < portCalls[1]) {
		t.Fatalf("published-port and broker-restart Docker call order = ports %v, stop %d, start %d, ready %d", portCalls, stopIndex, startIndex, restartReadyIndex)
	}

	stateStoppedIndex, stateRunningIndex := -1, -1
	stoppedSampleChecked := false
	brokerContainer := result.Nodes[0].BrokerContainer
	for index, call := range calls {
		if len(call) == 3 && call[0] == "fake-state" && call[1] == brokerContainer && call[2] == "false" && stateStoppedIndex < 0 {
			stateStoppedIndex = index
			continue
		}
		if stateStoppedIndex >= 0 && len(call) == 3 && call[0] == "fake-state" && call[1] == brokerContainer && call[2] == "true" {
			stateRunningIndex = index
			break
		}
	}
	if stateStoppedIndex >= 0 && stateRunningIndex > stateStoppedIndex {
		for index := stateStoppedIndex + 1; index < stateRunningIndex; index++ {
			call := calls[index]
			if len(call) == 5 && call[0] == "stats" && call[4] == brokerContainer {
				for inspectIndex := index + 1; inspectIndex < stateRunningIndex; inspectIndex++ {
					inspect := calls[inspectIndex]
					if len(inspect) == 4 && inspect[0] == "inspect" && inspect[2] == "{{.State.Running}}" && inspect[3] == brokerContainer {
						stoppedSampleChecked = true
						break
					}
				}
			}
		}
	}
	if !stoppedSampleChecked {
		t.Fatal("broker stats failure while stopped was not checked against Docker running state")
	}
}

func TestRunClusterTargetRecordsControllerFailure(t *testing.T) {
	fakeDocker := installFakeDocker(t)
	options := validBP07ClusterOptions(t)
	options.Controller = func(context.Context, []ClusterNodeResult) error {
		if err := os.WriteFile(fakeDocker.releasePath, []byte("done"), 0o600); err != nil {
			return err
		}
		return errors.New("injected controller failure")
	}

	result, err := RunClusterTarget(context.Background(), options)
	if err != nil {
		t.Fatalf("RunClusterTarget() error = %v", err)
	}
	if result.Success || !strings.Contains(result.FailureReason, "cluster controller failed: injected controller failure") {
		t.Fatalf("RunClusterTarget() result = %#v, want controller failure", result)
	}
}

func TestBP07RunnerTargetValidation(t *testing.T) {
	cluster := validClusterOptions()
	cluster.RunnerTarget = "bp07-runner"
	if err := validateClusterTargetOptions(cluster); err != nil {
		t.Fatalf("validateClusterTargetOptions(bp07-runner) error = %v", err)
	}
	cluster.RunnerTarget = "bp99-runner"
	if err := validateClusterTargetOptions(cluster); err == nil {
		t.Fatal("validateClusterTargetOptions() accepted unsupported runner target")
	}

	single := validTargetOptions()
	single.RunnerTarget = "bp07-runner"
	if err := validateTargetOptions(single); err == nil {
		t.Fatal("validateTargetOptions() accepted bp07-runner")
	}
}

func TestResolveClusterPublishedPortsRejectsEmptyMapping(t *testing.T) {
	installFakeDocker(t)
	t.Setenv(fakeDockerEmptyPortEnv, "1")
	if _, err := resolveClusterPublishedPorts(context.Background(), "bp07-owner", []string{"8080"}); err == nil {
		t.Fatal("resolveClusterPublishedPorts() accepted an empty Docker mapping")
	}
}

func validBP07ClusterOptions(t *testing.T) ClusterTargetOptions {
	t.Helper()
	options := validClusterOptions()
	options.BenchmarkID = "bp07"
	options.RunID = "restart-test"
	options.Target = "narumq"
	options.RunnerTarget = "bp07-runner"
	options.ArtifactDirectory = t.TempDir()
	options.Timeout = 15 * time.Second
	options.StartupTimeout = 5 * time.Second
	for index := range options.Nodes {
		options.Nodes[index].ReadyLogMarker = "MQTT TCP listener started"
	}
	return options
}

type fakeDockerPaths struct {
	callsPath   string
	releasePath string
}

func installFakeDocker(t *testing.T) fakeDockerPaths {
	t.Helper()
	workingDirectory := t.TempDir()
	binaryDirectory := filepath.Join(workingDirectory, "bin")
	stateDirectory := filepath.Join(workingDirectory, "state")
	if err := os.MkdirAll(binaryDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stateDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	name := "docker"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	fakeDockerBinary.Do(func() {
		directory, err := os.MkdirTemp("", "narumq-fake-docker-")
		if err != nil {
			fakeDockerBinary.err = err
			return
		}
		fakeDockerBinary.directory = directory
		path := filepath.Join(directory, name)
		command := exec.Command("go", "test", "-c", "-cover=false", "-o", path, "narumq/tests/benchmark/internal/environment")
		if output, err := command.CombinedOutput(); err != nil {
			fakeDockerBinary.err = fmt.Errorf("build fake Docker: %w: %s", err, output)
			return
		}
		fakeDockerBinary.data, fakeDockerBinary.err = os.ReadFile(path)
	})
	if fakeDockerBinary.err != nil {
		t.Fatal(fakeDockerBinary.err)
	}
	if err := os.WriteFile(filepath.Join(binaryDirectory, name), fakeDockerBinary.data, 0o755); err != nil {
		t.Fatal(err)
	}
	callsPath := filepath.Join(workingDirectory, "docker-calls.jsonl")
	releasePath := filepath.Join(workingDirectory, "runner-release")
	t.Setenv("PATH", binaryDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(fakeDockerModeEnv, "1")
	t.Setenv(fakeDockerStateEnv, stateDirectory)
	t.Setenv(fakeDockerCallsEnv, callsPath)
	t.Setenv(fakeDockerReleaseEnv, releasePath)
	t.Setenv(fakeDockerEmptyPortEnv, "")
	t.Setenv(fakeDockerStartAfterStoppedStatsEnv, "")
	return fakeDockerPaths{callsPath: callsPath, releasePath: releasePath}
}

func runFakeDocker(args []string) int {
	if len(args) == 0 {
		return fakeDockerError("docker command is required")
	}
	if err := appendFakeDockerCall(args); err != nil {
		return fakeDockerError(err.Error())
	}
	switch args[0] {
	case "build":
		fmt.Println("built")
		return 0
	case "image":
		if len(args) >= 4 && args[1] == "inspect" && args[2] == "--format" {
			if strings.Contains(args[3], ".RepoDigests") {
				fmt.Println("[]")
			} else {
				fmt.Println("sha256:fake-image")
			}
			return 0
		}
	case "network", "volume":
		if len(args) >= 3 && args[1] == "create" {
			fmt.Println(args[2])
		}
		return 0
	case "run":
		if name := valueAfter(args, "--name"); name != "" && containsToken(args, "--detach") {
			if err := writeFakeDockerRunning(name, true); err != nil {
				return fakeDockerError(err.Error())
			}
			fmt.Println("fake-container-id")
			return 0
		}
		releasePath := os.Getenv(fakeDockerReleaseEnv)
		if releasePath != "" && !waitForFakeDockerRelease(releasePath, 10*time.Second) {
			return fakeDockerError("runner release signal timed out")
		}
		time.Sleep(20 * time.Millisecond)
		fmt.Println("runner finished")
		return 0
	case "port":
		if os.Getenv(fakeDockerEmptyPortEnv) == "1" {
			return 0
		}
		if len(args) >= 3 && fakeDockerIsRunning(args[1]) {
			fmt.Println("127.0.0.1:49152")
			fmt.Println("[::]:49152")
			return 0
		}
		return fakeDockerError("network owner is stopped")
	case "inspect":
		if len(args) >= 4 && args[1] == "--format" {
			running := fakeDockerIsRunning(args[3])
			switch args[2] {
			case "{{.State.Status}}":
				if running {
					fmt.Println("running")
				} else {
					fmt.Println("exited")
				}
			case "{{.State.Running}}":
				fmt.Println(strconv.FormatBool(running))
			default:
				return fakeDockerError("unexpected inspect format " + args[2])
			}
			return 0
		}
		fmt.Print(`[{"Id":"fake-container","Config":{"Env":[]}}]`)
		return 0
	case "stats":
		container := args[len(args)-1]
		if !fakeDockerIsRunning(container) {
			if os.Getenv(fakeDockerStartAfterStoppedStatsEnv) == "1" {
				if err := writeFakeDockerRunning(container, true); err != nil {
					return fakeDockerError(err.Error())
				}
			}
			return fakeDockerError("container is stopped")
		}
		fmt.Println(`{"CPUPerc":"1.0%","MemUsage":"1MiB / 2GiB","NetIO":"2kB / 2kB","BlockIO":"0B / 0B"}`)
		return 0
	case "logs":
		fmt.Println("MQTT TCP listener started")
		return 0
	case "stop":
		if len(args) >= 4 {
			if err := writeFakeDockerRunning(args[len(args)-1], false); err != nil {
				return fakeDockerError(err.Error())
			}
		}
		return 0
	case "start":
		if len(args) >= 2 {
			time.Sleep(DefaultStatsSamplePeriod + 500*time.Millisecond)
			if err := writeFakeDockerRunning(args[1], true); err != nil {
				return fakeDockerError(err.Error())
			}
			fmt.Println(args[1])
			return 0
		}
	case "rm":
		return 0
	}
	return fakeDockerError("unexpected command: " + strings.Join(args, " "))
}

func appendFakeDockerCall(args []string) error {
	encoded, err := json.Marshal(args)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(os.Getenv(fakeDockerCallsEnv), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(append(encoded, '\n'))
	return err
}

func fakeDockerError(message string) int {
	fmt.Fprintln(os.Stderr, message)
	return 1
}

func fakeDockerStatePath(container string) string {
	return filepath.Join(os.Getenv(fakeDockerStateEnv), container+".running")
}

func fakeDockerIsRunning(container string) bool {
	status, err := os.ReadFile(fakeDockerStatePath(container))
	return err != nil || strings.TrimSpace(string(status)) == "true"
}

func writeFakeDockerRunning(container string, running bool) error {
	if err := os.WriteFile(fakeDockerStatePath(container), []byte(strconv.FormatBool(running)), 0o600); err != nil {
		return err
	}
	return appendFakeDockerCall([]string{"fake-state", container, strconv.FormatBool(running)})
}

func waitForFakeDockerRelease(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func readFakeDockerCalls(t *testing.T, path string) [][]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var calls [][]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var call []string
		if err := json.Unmarshal(scanner.Bytes(), &call); err != nil {
			t.Fatalf("decode fake Docker call: %v", err)
		}
		calls = append(calls, call)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return calls
}

func findNamedContainerRun(t *testing.T, calls [][]string, name string) []string {
	t.Helper()
	for _, call := range calls {
		if len(call) > 0 && call[0] == "run" && valueAfter(call, "--name") == name {
			return call
		}
	}
	t.Fatalf("docker run for container %q was not recorded", name)
	return nil
}

func valueAfter(values []string, key string) string {
	for index := 0; index+1 < len(values); index++ {
		if values[index] == key {
			return values[index+1]
		}
	}
	return ""
}

func indexOf(values []string, want string) int {
	for index, value := range values {
		if value == want {
			return index
		}
	}
	return -1
}
