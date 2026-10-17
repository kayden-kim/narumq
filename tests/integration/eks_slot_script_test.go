package integration

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// eksSlotScriptRun is the observable result of one manifest shell script run in a sandbox.
type eksSlotScriptRun struct {
	ExitCode int
	Stderr   string
	// Exec holds the argument lines the fake broker (or process wrapper) received, or nil when it never ran.
	Exec []string
	// Curl holds the argument lines of the first fake curl call, or nil when no HTTP request was made.
	Curl []string
	// ConfigsDir is the sandbox directory that replaces /configs.
	ConfigsDir string
}

// runEKSSlotScript executes a manifest shell script in a sandbox that replaces only absolute
// paths and external programs. The script's own slot lookup, validation and exec logic run unchanged.
//
// slots maps ConfigMap keys (for example "slot-2" or "node-4.json") to file contents.
// execPath is the manifest text of the program the script execs; it is replaced by a fake that
// records its arguments. Extra replacements may redirect further absolute paths.
func runEKSSlotScript(t *testing.T, script, hostname string, slots map[string]string, execPath string, extra ...string) eksSlotScriptRun {
	t.Helper()
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("sh is not available, skipping the manifest shell behaviour check: %v", err)
	}
	root := t.TempDir()
	configs := filepath.Join(root, "configs")
	bin := filepath.Join(root, "bin")
	for _, dir := range []string{configs, bin} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range slots {
		if err := os.WriteFile(filepath.Join(configs, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	execRecord := filepath.Join(root, "exec.args")
	curlRecord := filepath.Join(root, "curl.args")
	token := filepath.Join(root, "admin.token")
	uuid := filepath.Join(root, "uuid")
	for path, content := range map[string]string{token: "token", uuid: "00000000-0000-0000-0000-000000000000"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	slash := filepath.ToSlash
	fakeExec := filepath.Join(bin, "fake-exec")
	write(fakeExec, "#!/bin/sh\nprintf '%s\\n' \"$@\" > '"+slash(execRecord)+"'\n")
	fakeHostname := filepath.Join(bin, "hostname")
	fakeCurl := filepath.Join(bin, "curl")
	write(fakeHostname, "#!/bin/sh\necho '"+hostname+"'\n")
	write(fakeCurl, "#!/bin/sh\n"+
		"[ -e '"+slash(curlRecord)+"' ] || printf '%s\\n' \"$@\" > '"+slash(curlRecord)+"'\n"+
		"cat > /dev/null\n"+
		"printf '%s' '{\"phase\":\"COMPLETED\",\"safeToStop\":true}HTTP:200'\n")

	// The MSYS shell on Windows may resolve hostname and curl from its own directories before PATH,
	// so the fakes are called by absolute path.
	pairs := []string{
		execPath, slash(fakeExec),
		"$(hostname |", "$(" + slash(fakeHostname) + " |",
		"| curl ", "| " + slash(fakeCurl) + " ",
		"/configs/", slash(configs) + "/",
		"/secrets/admin.token", slash(token),
		"/proc/sys/kernel/random/uuid", slash(uuid),
	}
	pairs = append(pairs, extra...)
	sandboxed := strings.NewReplacer(pairs...).Replace(script)

	cmd := exec.Command(shell, "-c", sandboxed)
	cmd.Env = append(os.Environ(), "HOSTNAME="+hostname)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	result := eksSlotScriptRun{ConfigsDir: slash(configs)}
	if err := cmd.Run(); err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run sandboxed script on %s: %v", runtime.GOOS, err)
		}
		result.ExitCode = exitErr.ExitCode()
	}
	result.Stderr = stderr.String()
	readLines := func(path string) []string {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			t.Fatal(err)
		}
		return strings.Split(strings.TrimRight(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n"), "\n")
	}
	result.Exec = readLines(execRecord)
	result.Curl = readLines(curlRecord)
	return result
}

// invalidEKSSlotValues are slot file contents the manifests must refuse. A nil entry is a missing file.
var invalidEKSSlotValues = map[string]*string{
	"missing file":         nil,
	"empty":                new(""),
	"zero":                 new("0"),
	"leading zero":         new("03"),
	"negative":             new("-1"),
	"letters":              new("x3"),
	"two numbers":          new("1 2"),
	"path traversal":       new("../3"),
	"trailing json suffix": new("3.json"),
}

// assertEKSMainSlotBehaviour runs the broker start script against valid and invalid slot files.
// wantExec returns the argument lines the script must hand to the program it execs.
func assertEKSMainSlotBehaviour(t *testing.T, script, execPath string, wantExec func(configs, id string) []string) {
	t.Helper()
	for _, valid := range []struct {
		name, host string
		slots      map[string]string
		wantID     string
	}{
		{"initial slot 2", "narumq-2", map[string]string{"slot-0": "1", "slot-1": "2", "slot-2": "3"}, "3"},
		{"initial slot 0", "narumq-0", map[string]string{"slot-0": "1", "slot-1": "2", "slot-2": "3"}, "1"},
		{"reseeded slot 2", "narumq-2", map[string]string{"slot-0": "1", "slot-1": "2", "slot-2": "4"}, "4"},
	} {
		run := runEKSSlotScript(t, script, valid.host, valid.slots, execPath)
		if run.ExitCode != 0 || !reflect.DeepEqual(run.Exec, wantExec(run.ConfigsDir, valid.wantID)) {
			t.Fatalf("%s: exit=%d exec=%q stderr=%q, want exit 0 exec=%q", valid.name, run.ExitCode, run.Exec, run.Stderr, wantExec(run.ConfigsDir, valid.wantID))
		}
	}
	for name, value := range invalidEKSSlotValues {
		slots := map[string]string{"slot-0": "1", "slot-1": "2"}
		if value != nil {
			slots["slot-2"] = *value
		}
		run := runEKSSlotScript(t, script, "narumq-2", slots, execPath)
		if run.ExitCode != 1 || run.Exec != nil || !strings.Contains(run.Stderr, "member slot") {
			t.Fatalf("invalid slot (%s): exit=%d exec=%q stderr=%q, want exit 1, no exec and a member slot message", name, run.ExitCode, run.Exec, run.Stderr)
		}
	}
}

// assertEKSPreStopSlotBehaviour runs the preStop script and checks that it drains the slot's
// member ID and sends no request when the slot is invalid.
func assertEKSPreStopSlotBehaviour(t *testing.T, script string) {
	t.Helper()
	for _, valid := range []struct {
		name, host, slot2, wantURL string
	}{
		{"initial slot 2", "narumq-2", "3", "http://127.0.0.1:8080/api/v1/nodes/3/drain"},
		{"reseeded slot 2", "narumq-2", "4", "http://127.0.0.1:8080/api/v1/nodes/4/drain"},
	} {
		run := runEKSSlotScript(t, script, valid.host, map[string]string{"slot-0": "1", "slot-1": "2", "slot-2": valid.slot2}, "/narumq")
		if run.ExitCode != 0 || !slices.Contains(run.Curl, valid.wantURL) {
			t.Fatalf("preStop %s: exit=%d curl=%q stderr=%q, want exit 0 and a request to %s", valid.name, run.ExitCode, run.Curl, run.Stderr, valid.wantURL)
		}
	}
	for name, value := range invalidEKSSlotValues {
		slots := map[string]string{"slot-0": "1", "slot-1": "2"}
		if value != nil {
			slots["slot-2"] = *value
		}
		run := runEKSSlotScript(t, script, "narumq-2", slots, "/narumq")
		if run.ExitCode != 1 || run.Curl != nil || !strings.Contains(run.Stderr, "member slot") {
			t.Fatalf("preStop invalid slot (%s): exit=%d curl=%q stderr=%q, want exit 1, no request and a member slot message", name, run.ExitCode, run.Curl, run.Stderr)
		}
	}
}
