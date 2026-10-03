package raft

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNodeHeldWALSyncDoesNotBlockTick(t *testing.T) {
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	original := filePersistenceOps
	entered, release := make(chan struct{}), make(chan struct{})
	filePersistenceOps.sync = func(file *os.File) error {
		if filepath.Base(file.Name()) == walName {
			select {
			case <-entered:
			default:
				close(entered)
				<-release
			}
		}
		return original.sync(file)
	}
	readyDone := make(chan error, 1)
	go func() { _, err := node.ProcessReady(); readyDone <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		<-readyDone
		_ = node.Close()
		filePersistenceOps = original
		t.Fatal("WAL sync was not reached")
	}
	tickDone := make(chan error, 1)
	go func() { tickDone <- node.Tick() }()
	select {
	case err := <-tickDone:
		if err != nil {
			t.Errorf("tick while WAL sync is held: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("logical tick blocked behind WAL fsync")
	}
	close(release)
	if err := <-readyDone; err != nil {
		t.Error(err)
	}
	if err := node.Close(); err != nil {
		t.Error(err)
	}
	filePersistenceOps = original
}
