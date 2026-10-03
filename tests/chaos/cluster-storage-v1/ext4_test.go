package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestExt4MountEvidenceRejectsWrongStorageAndIncompleteIdentity(t *testing.T) {
	valid := ext4MountEvidence{Event: "ext4-mount", Kernel: "6.6.0", Filesystem: "ext4", MountOptions: "rw,relatime,data=ordered", LoopDevice: "/dev/loop7", ImageMiB: 256, BrokerSHA256: strings.Repeat("ab", 32)}
	for _, test := range []struct {
		name   string
		change func(*ext4MountEvidence)
	}{
		{"FUSE", func(event *ext4MountEvidence) { event.Filesystem = "fuse" }},
		{"tmpfs", func(event *ext4MountEvidence) { event.Filesystem = "tmpfs" }},
		{"wrong size", func(event *ext4MountEvidence) { event.ImageMiB = 1024 }},
		{"missing checksum", func(event *ext4MountEvidence) { event.BrokerSHA256 = "" }},
		{"invalid checksum", func(event *ext4MountEvidence) { event.BrokerSHA256 = strings.Repeat("zz", 32) }},
		{"missing kernel", func(event *ext4MountEvidence) { event.Kernel = "" }},
		{"wrong mount ordering", func(event *ext4MountEvidence) { event.MountOptions = "rw,data=writeback" }},
		{"wrong device", func(event *ext4MountEvidence) { event.LoopDevice = "/dev/sda" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := valid
			test.change(&event)
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseExt4Mount(encoded, 256); err == nil {
				t.Fatal("invalid mount was accepted as ext4 evidence")
			}
		})
	}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	mount, err := parseExt4Mount(encoded, 256)
	if err != nil || mount != valid {
		t.Fatalf("mount=%+v error=%v", mount, err)
	}
	if _, err := parseExt4Mount(append(append(encoded, '\n'), encoded...), 256); err == nil {
		t.Fatal("ambiguous repeated mounts were accepted")
	}
	if _, err := parseExt4Mount([]byte(`{"event":"broker-start"}`), 256); err == nil {
		t.Fatal("missing mount was accepted")
	}
}

func TestRecoveryEvidenceRejectsMissingSkippedFailedAndUnfinishedTests(t *testing.T) {
	var valid bytes.Buffer
	encoder := json.NewEncoder(&valid)
	for _, test := range requiredRecoveryTests {
		separator := strings.LastIndexByte(test, '.')
		if err := encoder.Encode(map[string]string{"Action": "pass", "Package": test[:separator], "Test": test[separator+1:]}); err != nil {
			t.Fatal(err)
		}
	}
	testPasses := append([]byte(nil), valid.Bytes()...)
	for _, name := range []string{"narumq/internal/adapters/raft", "narumq/tests/integration"} {
		if err := encoder.Encode(map[string]string{"Action": "pass", "Package": name}); err != nil {
			t.Fatal(err)
		}
	}
	passed, err := validateRecoveryTests(valid.Bytes())
	if err != nil || strings.Join(passed, "\n") != strings.Join(requiredRecoveryTests, "\n") {
		t.Fatalf("passed=%v error=%v", passed, err)
	}
	for _, test := range []struct {
		name string
		logs []byte
	}{
		{"missing tests", []byte(`{"Action":"pass","Package":"narumq/tests/integration"}`)},
		{"unfinished package", testPasses},
		{"nested skipped scenario", append(append([]byte(nil), valid.Bytes()...), []byte("{\"Action\":\"skip\",\"Package\":\"narumq/tests/integration\",\"Test\":\"TestDynamicClusterWALCompactionProcessAcceptance/restart\"}\n")...)},
		{"nested failed scenario", append(append([]byte(nil), valid.Bytes()...), []byte("{\"Action\":\"fail\",\"Package\":\"narumq/tests/integration\",\"Test\":\"TestClusterBackupRestoreProcessCrashAndResync/restore\"}\n")...)},
		{"truncated evidence", []byte(`{"Action":"pass"`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := validateRecoveryTests(test.logs); err == nil {
				t.Fatal("incomplete or unsuccessful recovery was accepted")
			}
		})
	}
}

func TestExt4CleanupRequiresConfirmedUnmountAndDetach(t *testing.T) {
	for _, logs := range []string{"", `{"event":"ext4-mount"}`, `{"event":"ext4-cleanup","status":"failed"}`} {
		if err := validateExt4Cleanup([]byte(logs)); err == nil {
			t.Fatalf("unconfirmed cleanup accepted: %s", logs)
		}
	}
	if err := validateExt4Cleanup([]byte(`{"event":"ext4-cleanup","status":"passed"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerCrashEvidenceRequiresSIGKILLOnTheCrashEvent(t *testing.T) {
	for _, logs := range []string{
		`{"event":"broker-exit","exitCode":137}`,
		`{"event":"broker-crash","exitCode":0}`,
		"{\"event\":\"broker-crash\",\"exitCode\":0}\n{\"event\":\"broker-exit\",\"exitCode\":137}",
		`{"event":"broker-crash"}`,
	} {
		if err := validateBrokerCrash([]byte(logs)); err == nil {
			t.Fatalf("unconfirmed SIGKILL accepted: %s", logs)
		}
	}
	if err := validateBrokerCrash([]byte(`{"event":"broker-crash","exitCode":137}`)); err != nil {
		t.Fatal(err)
	}
}
