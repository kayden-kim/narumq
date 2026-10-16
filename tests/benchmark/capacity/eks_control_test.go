package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Exercise the actual Python producer and Go consumer; their JSON types must agree.
func TestEKSKillMarkersDecodeInRunner(t *testing.T) {
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("Python is required for the EKS controller protocol test")
	}
	controller, err := filepath.Abs(filepath.Join("eks", "controller.py"))
	if err != nil {
		t.Fatal(err)
	}
	const producer = `import importlib.util,json,sys
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('capacity_controller',sys.argv[1])
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
print(json.dumps(m.kill_marker(sys.argv[2],2,'narumq-1','worker-b','2026-10-16T00:00:00Z')))
`
	for _, state := range []string{killStateKilling, killStateDown, killStateRestoring, killStateRestored, killStateFailed} {
		t.Run(state, func(t *testing.T) {
			data, err := exec.Command(python, "-B", "-c", producer, controller, state).CombinedOutput()
			if err != nil {
				t.Fatalf("Python marker producer: %v: %s", err, data)
			}
			dir := t.TempDir()
			if err := os.WriteFile(markerPath(dir, 1, 1, "kill"), data, 0o600); err != nil {
				t.Fatal(err)
			}
			phase := newKillPhase(dir, 1, 1)
			got, first := phase.poll()
			if got != state || !first || phase.lastMarker.Target.NodeID != "2" || phase.lastMarker.Target.Pod != "narumq-1" {
				t.Fatalf("runner did not observe Python marker: state=%q first=%v target=%+v", got, first, phase.lastMarker.Target)
			}
		})
	}
}
