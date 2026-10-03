package raft

import (
	"fmt"
	"slices"
	"time"

	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	walAppendCapacity = 64
	walAppendBytes    = 4 << 20
)

type walAppendRequest struct {
	sequence      uint64
	bytes         int
	record        walRecord
	responses     []*pb.Message
	messages      []MessageDTO
	boundary      PersistenceBoundaryDTO
	normalEntries [][]byte
}

type walAppendResult struct {
	request *walAppendRequest
	err     error
}

// WALAppendWorker owns disk writes only. Memory state and RawNode remain on Node.
// Queue accounting includes work until Node consumes its completion.
type WALAppendWorker struct {
	requests chan *walAppendRequest
	results  chan walAppendResult
	ready    chan struct{}
	activity func(bool)
	done     chan struct{}
	store    *stableStore
	// Published before closing requests; read after that close or worker join.
	finalAppend *walAppendRequest
	finalError  error
	hook        func(PersistenceBoundaryDTO)
}

func newWALAppendWorker(store *stableStore, hook func(PersistenceBoundaryDTO), activity func(bool)) *WALAppendWorker {
	worker := &WALAppendWorker{
		requests: make(chan *walAppendRequest, walAppendCapacity),
		results:  make(chan walAppendResult, walAppendCapacity),
		ready:    make(chan struct{}, 1), done: make(chan struct{}), store: store, hook: hook, activity: activity,
	}
	go worker.run()
	return worker
}

func (worker *WALAppendWorker) run() {
	defer close(worker.done)
	var carried *walAppendRequest
	for {
		first := carried
		carried = nil
		if first == nil {
			var ok bool
			first, ok = <-worker.requests
			if !ok {
				if worker.finalAppend != nil {
					worker.finalError = worker.persistBatch([]*walAppendRequest{worker.finalAppend})
				}
				return
			}
		}
		var batch [walAppendCapacity]*walAppendRequest
		batch[0] = first
		count, size := 1, first.bytes
		// Incoming snapshots are barriers, not ordinary group-commit records.
		if first.record.snapshot == nil {
		collect:
			for count < len(batch) {
				select {
				case next, ok := <-worker.requests:
					if !ok {
						break collect
					}
					if next.record.snapshot != nil || size+next.bytes > walAppendBytes {
						carried = next
						break collect
					}
					batch[count] = next
					count++
					size += next.bytes
				default:
					break collect
				}
			}
		}
		if worker.activity != nil {
			worker.activity(true)
		}
		err := worker.persistBatch(batch[:count])
		if worker.activity != nil {
			worker.activity(false)
		}
		for _, request := range batch[:count] {
			worker.results <- walAppendResult{request: request, err: err}
		}
		select {
		case worker.ready <- struct{}{}:
		default:
		}
		if err != nil {
			return // No subsequent request can succeed after a storage failure.
		}
	}
}

func (worker *WALAppendWorker) persistBatch(batch []*walAppendRequest) error {
	frames, entries, frameBytes := 0, 0, 0
	for _, request := range batch {
		if worker.hook != nil && (len(request.boundary.ConfigurationActions) != 0 || request.boundary.InstallsSnapshot) {
			boundary := request.boundary
			boundary.Stage = PersistenceBeforeReady
			worker.hook(boundary)
		}
		if !request.record.persistent() {
			continue
		}
		if worker.store.walHook != nil {
			request.normalEntries = normalEntryPayloads(request.record.entries)
		}
		written, err := worker.store.writeWALFrame(request.record, request.normalEntries)
		if err != nil {
			return err
		}
		frames++
		entries += len(request.record.entries)
		frameBytes += written
	}
	if frames != 0 {
		for _, request := range batch {
			worker.observeEntries(request, WALBeforeSync)
		}
		var started time.Time
		if worker.store.walSyncObserver != nil {
			started = time.Now()
		}
		if err := filePersistenceOps.sync(worker.store.wal); err != nil {
			return fmt.Errorf("sync WAL: %w", err)
		}
		if worker.store.walSyncObserver != nil {
			worker.store.walSyncObserver(WALSyncDTO{Duration: time.Since(started), Entries: entries, FrameBytes: frameBytes})
		}
		for _, request := range batch {
			worker.observeEntries(request, WALAfterSync)
		}
	}
	for _, request := range batch {
		if request.record.snapshot != nil {
			if err := persistSnapshotFileMode(worker.store.directory, request.record.snapshot, worker.store.dynamic, nil); err != nil {
				return err
			}
		}
		if worker.hook != nil && (len(request.boundary.ConfigurationActions) != 0 || request.boundary.InstallsSnapshot) {
			boundary := request.boundary
			boundary.Stage = PersistenceAfterReady
			worker.hook(boundary)
		}
	}
	return nil
}

func (worker *WALAppendWorker) observeEntries(request *walAppendRequest, stage WALBoundaryStage) {
	if worker.store.walHook == nil {
		return
	}
	if len(request.normalEntries) != 0 {
		worker.store.walHook(WALBoundaryDTO{Stage: stage, Entries: request.normalEntries})
	}
}

func normalEntryPayloads(entries []*pb.Entry) [][]byte {
	var payloads [][]byte
	for _, entry := range entries {
		if entry.GetType() == pb.EntryNormal && len(entry.GetData()) != 0 {
			payloads = append(payloads, entry.GetData())
		}
	}
	return payloads
}

func (n *Node) newAppendRequest(message *pb.Message) (*walAppendRequest, error) {
	var hardState *pb.HardState
	if message.Term != nil || message.Vote != nil || message.Commit != nil {
		hardState = &pb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit}
	}
	record, err := readyRecordMode(etcdraft.Ready{HardState: hardState, Entries: message.Entries, Snapshot: message.Snapshot}, n.dynamic)
	if err != nil {
		return nil, err
	}
	n.appendSequence++
	request := &walAppendRequest{
		sequence: n.appendSequence, record: record, responses: message.Responses,
		bytes: 128,
	}
	for _, entry := range record.entries {
		request.bytes += proto.Size(entry) + 4
	}
	if record.snapshot != nil {
		request.bytes += proto.Size(record.snapshot) + 4
		request.boundary.InstallsSnapshot = true
	}
	for _, response := range request.responses {
		responseBytes := proto.Size(response)
		if record.snapshot != nil && response.Snapshot == record.snapshot {
			responseBytes -= proto.Size(record.snapshot) // Shared immutable payload.
		}
		request.bytes += responseBytes + 8
	}
	if n.writer.hook != nil && n.dynamic {
		for _, entry := range record.entries {
			if entry.GetType() != pb.EntryConfChangeV2 {
				continue
			}
			change, err := decodeConfChange(entry)
			if err != nil {
				return nil, err
			}
			action, _, err := validateDynamicConfChange(change)
			if err != nil {
				return nil, err
			}
			request.boundary.ConfigurationActions = append(request.boundary.ConfigurationActions, action)
		}
	}
	// Responses are immutable and owned by this Ready. Copy the slice header's
	// backing array because future Ready work may append to the library slice.
	request.responses = slices.Clone(request.responses)
	return request, nil
}
