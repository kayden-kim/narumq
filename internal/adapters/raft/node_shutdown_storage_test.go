package raft

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestNodeCloseFlushesHeldAppendOrReportsFailure(t *testing.T) {
	for _, scenario := range []struct {
		name         string
		failFinal    bool
		failPrior    bool
		consumePrior bool
	}{
		{name: "persist"},
		{name: "sync-error", failFinal: true},
		{name: "consumed-prior-error", failPrior: true, consumePrior: true},
		{name: "unconsumed-prior-error", failPrior: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			node := storageBoundaryNode(t)
			entered, release := holdWALSync(t, node)
			var heldIndex atomic.Uint64
			var failFinal atomic.Bool
			var failPrior atomic.Bool
			node.store.walHook = func(boundary WALBoundaryDTO) {
				if !scenario.failFinal || boundary.Stage != WALBeforeSync {
					return
				}
				for _, payload := range boundary.Entries {
					if len(payload) != 0 && uint64(payload[0]) == heldIndex.Load() {
						failFinal.Store(true)
					}
				}
			}
			heldSync := filePersistenceOps.sync
			filePersistenceOps.sync = func(file *os.File) error {
				err := heldSync(file)
				if (failFinal.Load() || failPrior.Load()) && filepath.Base(file.Name()) == walName {
					return os.ErrPermission
				}
				return err
			}
			last := uint64(3)
			for !node.storageSaturated() {
				last++
				payload := make([]byte, 64<<10)
				payload[0] = byte(last)
				previousTerm := uint64(2)
				if last == 4 {
					previousTerm = 1
				}
				stepStorageAppend(t, node, 1, 2, last, previousTerm, 3, payload)
				if _, err := node.ProcessReady(); err != nil {
					t.Fatal(err)
				}
				if last == 4 {
					waitStorageBoundary(t, entered)
				}
			}
			if node.heldAppend == nil {
				t.Fatal("fixture did not retain the byte-budget append")
			}
			heldIndex.Store(last)
			closed := make(chan error, 1)
			if scenario.failPrior {
				failPrior.Store(true)
				release()
				if scenario.consumePrior {
					if _, err := processReadyForTest(t, node); !errors.Is(err, ErrStorage) {
						t.Fatalf("prior sync failure = %v", err)
					}
					if node.heldAppend == nil || len(node.writer.requests) == 0 {
						t.Fatal("prior failure did not leave the held append and queued work unprocessed")
					}
				} else {
					deadline := time.NewTimer(5 * time.Second)
					defer deadline.Stop()
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for len(node.writer.results) == 0 {
						select {
						case <-deadline.C:
							t.Fatal("prior sync failure was not queued within 5 seconds")
						case <-ticker.C:
						}
					}
				}
			}
			go func() { closed <- node.Close() }()
			release()
			var err error
			select {
			case err = <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("Close blocked with held append and unconsumed completions")
			}
			if scenario.failFinal || scenario.failPrior {
				if !errors.Is(err, ErrStorage) || !errors.Is(err, os.ErrPermission) {
					t.Fatalf("held append sync failure on Close = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			records, err := readWAL(filepath.Join(node.store.directory, walName))
			if err != nil {
				t.Fatal(err)
			}
			next := uint64(4)
			for _, record := range records {
				for _, entry := range record.entries {
					if entry.GetIndex() < 4 {
						continue
					}
					if entry.GetIndex() != next || entry.Data[0] != byte(next) {
						t.Fatalf("shutdown WAL entry=%d want=%d", entry.GetIndex(), next)
					}
					next++
				}
			}
			if next != last+1 {
				t.Fatalf("held append lost on Close: next=%d want=%d", next, last+1)
			}
		})
	}
}
