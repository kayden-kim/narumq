package raft

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

// Scenarios: management-api-and-observability.md:MGT-SCN-032, cluster-session-and-delivery.md:CLU-SCN-117
func TestWALWorkerGroupsQueuedFramesAndFailsClosed(t *testing.T) {
	for _, failure := range []string{"none", "write", "sync"} {
		t.Run(failure, func(t *testing.T) {
			store, err := openStableStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			original := filePersistenceOps
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			syncs, writes := 0, 0 // Owned by the single writer until it joins.
			filePersistenceOps.sync = func(file *os.File) error {
				if filepath.Base(file.Name()) == walName {
					syncs++
					if syncs == 1 {
						close(entered)
						<-release
					}
					if failure == "sync" && syncs == 2 {
						return errors.New("group sync failure")
					}
				}
				return original.sync(file)
			}
			filePersistenceOps.write = func(writer io.Writer, data []byte) (int, error) {
				writes++
				if failure == "write" && writes == 5 {
					return 0, errors.New("middle frame write failure")
				}
				return original.write(writer, data)
			}
			var observations []WALSyncDTO
			store.walSyncObserver = func(observation WALSyncDTO) { observations = append(observations, observation) }
			worker := newWALAppendWorker(store, nil, nil)
			closed := false
			t.Cleanup(func() {
				unblock()
				if !closed {
					close(worker.requests)
				}
				<-worker.done
				filePersistenceOps = original
				_ = store.close()
			})
			request := func(index uint64) *walAppendRequest {
				return &walAppendRequest{
					sequence: index, bytes: 128,
					record: walRecord{
						hardState: &pb.HardState{Term: new(uint64(1)), Commit: new(index)},
						entries:   []*pb.Entry{{Index: new(index), Term: new(uint64(1)), Type: pb.EntryNormal.Enum(), Data: []byte{byte(index)}}},
					},
				}
			}
			worker.requests <- request(1)
			waitStorageBoundary(t, entered)
			for index := uint64(2); index <= 4; index++ {
				worker.requests <- request(index)
			}
			close(worker.requests)
			closed = true
			unblock()
			select {
			case <-worker.done:
			case <-time.After(5 * time.Second):
				t.Fatal("WAL worker did not join after draining accepted work")
			}
			for index := uint64(1); index <= 4; index++ {
				result := <-worker.results
				if result.request.sequence != index {
					t.Fatalf("completion sequence=%d want=%d", result.request.sequence, index)
				}
				if index == 1 || failure == "none" {
					if result.err != nil {
						t.Fatal(result.err)
					}
				} else if result.err == nil {
					t.Fatal("failed group released a successful completion")
				}
			}
			filePersistenceOps = original
			if failure != "none" {
				if len(observations) != 1 || observations[0].Entries != 1 {
					t.Fatal("failed group was reported as durably synced")
				}
				return
			}
			if syncs != 2 || len(observations) != 2 || observations[0].Entries != 1 || observations[1].Entries != 3 {
				t.Fatalf("grouped syncs=%d observations=%+v", syncs, observations)
			}
			if err := store.close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := openStableStore(store.directory)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.close()
			if recovered.hardState.GetCommit() != 4 || recovered.appliedIndex != 4 {
				t.Fatal("group commit frontier did not survive reopen")
			}
			for index, command := range recovered.commands {
				if len(command) != 1 || command[0] != byte(index+1) {
					t.Fatalf("recovered command %d=%v", index, command)
				}
			}
			if len(recovered.commands) != 4 {
				t.Fatalf("recovered command count=%d want=4", len(recovered.commands))
			}
		})
	}
}
