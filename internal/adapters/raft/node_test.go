package raft

import (
	"errors"
	"path/filepath"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"os"
)

func TestNodeConfigAndLifecycle(t *testing.T) {
	tests := []Config{
		{},
		{NodeID: 1, PeerIDs: []NodeID{1, 2}},
		{NodeID: 1, PeerIDs: []NodeID{1, 1, 2}},
		{NodeID: 1, PeerIDs: []NodeID{1, 2, 0}},
		{NodeID: 4, PeerIDs: []NodeID{1, 2, 3}},
	}
	for index, config := range tests {
		t.Run("invalid-"+string(rune('a'+index)), func(t *testing.T) {
			config.Directory = t.TempDir()
			_, err := Open(config)
			if !errors.Is(err, ErrConfigMismatch) {
				t.Fatalf("Open() error = %v, want ErrConfigMismatch", err)
			}
		})
	}

	directory := filepath.Join(t.TempDir(), "node")
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{3, 1, 2}, Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := node.ProcessReady()
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Messages) != 0 || len(batch.Commands) != 0 {
		t.Fatalf("bootstrap batch = %#v, want no external messages or application commands", batch)
	}
	status := node.Status()
	if !status.Bootstrapped || status.AppliedIndex == 0 || status.CommitIndex != status.AppliedIndex {
		t.Fatalf("bootstrap status = %#v", status)
	}
	commands := node.AppliedCommands()
	if len(commands) != 0 {
		t.Fatalf("bootstrap commands = %#v, want empty", commands)
	}
	if err := node.Snapshot(); err != nil {
		t.Fatalf("same-index Snapshot() = %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.Tick(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Tick() after Close() = %v, want ErrClosed", err)
	}
	if _, err := node.ProcessReady(); !errors.Is(err, ErrClosed) {
		t.Fatalf("ProcessReady() after Close() = %v, want ErrClosed", err)
	}
}

func TestNodeMessageAndProposalValidation(t *testing.T) {
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: filepath.Join(t.TempDir(), "node")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	if err := node.Propose([]byte("command")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose() on follower = %v, want ErrNotLeader", err)
	}
	if err := node.Step(MessageDTO{From: 2, To: 1, Data: []byte{0xff}}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Step() malformed payload = %v, want ErrInvalidMessage", err)
	}
	message := &pb.Message{Type: pb.MsgApp.Enum(), From: new(uint64(2)), To: new(uint64(3))}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Step(MessageDTO{From: 2, To: 1, Data: data}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Step() metadata mismatch = %v, want ErrInvalidMessage", err)
	}
	if err := node.Propose(nil); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("empty proposal on follower = %v, want ErrNotLeader", err)
	}
	if err := node.ProposeForwarded([]byte("command")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ProposeForwarded() without a leader = %v, want ErrNotLeader", err)
	}
}
func TestNodeMessageValidationAndFailureState(t *testing.T) {
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	for _, message := range []MessageDTO{
		{},
		{From: 1, To: 1, Data: []byte{1}},
		{From: 2, To: 2, Data: []byte{1}},
		{From: 2, To: 1, Data: make([]byte, maxMessageSize+1)},
	} {
		if err := node.Step(message); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("Step(%#v) error = %v, want ErrInvalidMessage", message, err)
		}
	}
	valid := &pb.Message{Type: pb.MsgApp.Enum(), From: uint64Ptr(2), To: uint64Ptr(1)}
	data, err := proto.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Step(MessageDTO{From: 2, To: 1, Data: data}); err != nil && !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("valid peer Step() error = %v", err)
	}
	if err := node.ForgetLeader(); err != nil {
		t.Fatalf("ForgetLeader() error = %v", err)
	}
	node.fail(errors.New("injected failure"))
	if err := node.Tick(); !errors.Is(err, ErrFailed) {
		t.Fatalf("Tick() after failure = %v, want ErrFailed", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.ForgetLeader(); !errors.Is(err, ErrClosed) {
		t.Fatalf("ForgetLeader() after close = %v, want ErrClosed", err)
	}
}
func TestNodeReportPeerMessageFailureValidatesFixedVoter(t *testing.T) {
	node, err := Open(Config{NodeID: 1, PeerIDs: []NodeID{1, 2, 3}, Directory: filepath.Join(t.TempDir(), "node")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.ProcessReady(); err != nil {
		t.Fatal(err)
	}
	for _, peerID := range []NodeID{0, 1, 4} {
		if err := node.ReportPeerMessageFailure(peerID, false); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("ReportPeerMessageFailure(%d) = %v, want ErrInvalidMessage", peerID, err)
		}
	}
	for _, failure := range []struct {
		peerID     NodeID
		isSnapshot bool
	}{{peerID: 2}, {peerID: 3, isSnapshot: true}} {
		if err := node.ReportPeerMessageFailure(failure.peerID, failure.isSnapshot); err != nil {
			t.Fatalf("ReportPeerMessageFailure(%d, snapshot=%t): %v", failure.peerID, failure.isSnapshot, err)
		}
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.ReportPeerMessageFailure(2, false); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReportPeerMessageFailure() after close = %v, want ErrClosed", err)
	}
}

func TestNodeHelpersCloneAndFixedConfiguration(t *testing.T) {
	if fixedConfState(nil, []NodeID{1, 2, 3}) {
		t.Fatal("fixedConfState(nil) unexpectedly true")
	}
	if fixedConfState(&pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}, []NodeID{1, 2, 3}) {
		t.Fatal("fixedConfState with learner unexpectedly true")
	}
	state := &pb.ConfState{Voters: []uint64{3, 1, 2}}
	if !fixedConfState(state, []NodeID{1, 2, 3}) {
		t.Fatal("fixedConfState rejected sorted-equivalent voters")
	}
	commands := [][]byte{[]byte("one"), nil}
	clone := cloneCommands(commands)
	clone[0][0] = 'X'
	if string(commands[0]) != "one" {
		t.Fatal("cloneCommands shared payload")
	}
	entries := cloneEntries([]*pb.Entry{{Index: uint64Ptr(1), Data: []byte("entry")}})
	entries[0].Data[0] = 'X'
	if string(entries[0].Data) == "entry" {
		t.Fatal("cloneEntries did not permit independent mutation")
	}
	if !errors.Is((&Node{}).fail(errors.New("failure")), ErrFailed) {
		t.Fatal("fail() did not wrap ErrFailed")
	}
}

func TestNodeCommandResultsAreDeepCopies(t *testing.T) {
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		node, err := Open(Config{
			NodeID:    nodeID,
			PeerIDs:   []NodeID{1, 2, 3},
			Directory: filepath.Join(t.TempDir(), "node"),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[nodeID] = node
	}
	defer func() {
		for _, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("Close() = %v", err)
			}
		}
	}()

	var returnedCommands [][]byte
	pump := func() {
		t.Helper()
		var queue []MessageDTO
		for range 512 {
			progress := false
			for _, nodeID := range []NodeID{1, 2, 3} {
				batch, err := nodes[nodeID].ProcessReady()
				if err != nil {
					t.Fatalf("node %d ProcessReady() = %v", nodeID, err)
				}
				if len(batch.Messages) != 0 || len(batch.Commands) != 0 {
					progress = true
				}
				if returnedCommands == nil && len(batch.Commands) != 0 {
					returnedCommands = batch.Commands
				}
				queue = append(queue, batch.Messages...)
			}
			if len(queue) != 0 {
				pending := queue
				queue = nil
				for _, message := range pending {
					if err := nodes[message.To].Step(message); err != nil {
						t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
					}
					progress = true
				}
			}
			if !progress && len(queue) == 0 {
				return
			}
		}
		t.Fatal("node pump exceeded 512 rounds")
	}

	pump()
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pump()
	if err := nodes[1].Propose([]byte("command")); err != nil {
		t.Fatal(err)
	}
	pump()
	if len(returnedCommands) != 1 || string(returnedCommands[0]) != "command" {
		t.Fatalf("returned commands = %q, want command", returnedCommands)
	}

	returnedCommands[0][0] = 'X'
	for _, nodeID := range []NodeID{1, 2, 3} {
		if got := nodes[nodeID].AppliedCommands(); len(got) != 1 || string(got[0]) != "command" {
			t.Fatalf("node %d AppliedCommands after BatchDTO mutation = %q, want command", nodeID, got)
		}
	}
	applied := nodes[2].AppliedCommands()
	applied[0][0] = 'Y'
	if got := nodes[2].AppliedCommands(); len(got) != 1 || string(got[0]) != "command" {
		t.Fatalf("AppliedCommands mutation changed node state: %q", got)
	}
}

func TestWALBoundaryHookReportsNormalEntryStages(t *testing.T) {
	root := t.TempDir()
	directories := map[NodeID]string{
		1: filepath.Join(root, "node-1"),
		2: filepath.Join(root, "node-2"),
		3: filepath.Join(root, "node-3"),
	}
	var hookInvocations int
	var stages []WALBoundaryStage
	var walSizes []int64
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		config := Config{
			NodeID:    nodeID,
			PeerIDs:   []NodeID{1, 2, 3},
			Directory: directories[nodeID],
		}
		if nodeID == 1 {
			config.WALBoundaryHook = func(boundary WALBoundaryDTO) {
				hookInvocations++
				for _, entry := range boundary.Entries {
					if string(entry) != "cmd" {
						continue
					}
					info, err := os.Stat(filepath.Join(directories[1], walName))
					if err != nil {
						t.Errorf("stat WAL at stage %d: %v", boundary.Stage, err)
						return
					}
					stages = append(stages, boundary.Stage)
					walSizes = append(walSizes, info.Size())
				}
			}
		}
		node, err := Open(config)
		if err != nil {
			t.Fatal(err)
		}
		nodes[nodeID] = node
	}
	defer func() {
		for _, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("Close() = %v", err)
			}
		}
	}()

	pump := func() {
		t.Helper()
		var queue []MessageDTO
		for range 512 {
			progress := false
			for _, nodeID := range []NodeID{1, 2, 3} {
				batch, err := nodes[nodeID].ProcessReady()
				if err != nil {
					t.Fatalf("node %d ProcessReady() = %v", nodeID, err)
				}
				if len(batch.Messages) != 0 || len(batch.Commands) != 0 {
					progress = true
				}
				queue = append(queue, batch.Messages...)
			}
			if len(queue) != 0 {
				pending := queue
				queue = nil
				for _, message := range pending {
					if err := nodes[message.To].Step(message); err != nil {
						t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
					}
					progress = true
				}
			}
			if !progress && len(queue) == 0 {
				return
			}
		}
		t.Fatal("node pump exceeded 512 rounds")
	}

	pump()
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pump()
	if hookInvocations != 0 {
		t.Fatalf("WAL hook ran %d times for election entries: stages = %v", hookInvocations, stages)
	}
	if err := nodes[1].Propose([]byte("cmd")); err != nil {
		t.Fatal(err)
	}
	pump()

	wantStages := []WALBoundaryStage{WALBeforeWrite, WALBeforeSync, WALAfterSync}
	if hookInvocations != len(wantStages) {
		t.Fatalf("WAL hook invocation count = %d, want %d", hookInvocations, len(wantStages))
	}
	if len(stages) != len(wantStages) {
		t.Fatalf("WAL stages = %v, want %v", stages, wantStages)
	}
	for index, want := range wantStages {
		if stages[index] != want {
			t.Fatalf("WAL stages = %v, want %v", stages, wantStages)
		}
	}
	if walSizes[1] <= walSizes[0] {
		t.Fatalf("WAL size before sync = %d, must exceed size before write = %d", walSizes[1], walSizes[0])
	}
}

func TestWALSyncObserverReportsSuccessfulReadyFsyncs(t *testing.T) {
	root := t.TempDir()
	directories := map[NodeID]string{
		1: filepath.Join(root, "node-1"),
		2: filepath.Join(root, "node-2"),
		3: filepath.Join(root, "node-3"),
	}
	var observations []WALSyncDTO
	nodes := make(map[NodeID]*Node, 3)
	for _, nodeID := range []NodeID{1, 2, 3} {
		config := Config{
			NodeID:    nodeID,
			PeerIDs:   []NodeID{1, 2, 3},
			Directory: directories[nodeID],
		}
		if nodeID == 1 {
			config.WALSyncObserver = func(observation WALSyncDTO) {
				observations = append(observations, observation)
			}
		}
		node, err := Open(config)
		if err != nil {
			t.Fatal(err)
		}
		nodes[nodeID] = node
	}
	defer func() {
		for _, node := range nodes {
			if err := node.Close(); err != nil {
				t.Errorf("Close() = %v", err)
			}
		}
	}()

	pump := func() {
		t.Helper()
		var queue []MessageDTO
		for range 512 {
			progress := false
			for _, nodeID := range []NodeID{1, 2, 3} {
				batch, err := nodes[nodeID].ProcessReady()
				if err != nil {
					t.Fatalf("node %d ProcessReady() = %v", nodeID, err)
				}
				if len(batch.Messages) != 0 || len(batch.Commands) != 0 {
					progress = true
				}
				queue = append(queue, batch.Messages...)
			}
			if len(queue) != 0 {
				pending := queue
				queue = nil
				for _, message := range pending {
					if err := nodes[message.To].Step(message); err != nil {
						t.Fatalf("deliver %d -> %d: %v", message.From, message.To, err)
					}
					progress = true
				}
			}
			if !progress && len(queue) == 0 {
				return
			}
		}
		t.Fatal("node pump exceeded 512 rounds")
	}

	pump()
	if err := nodes[1].Campaign(); err != nil {
		t.Fatal(err)
	}
	pump()
	if err := nodes[1].Propose([]byte("cmd")); err != nil {
		t.Fatal(err)
	}
	pump()

	var observedProposal bool
	for _, observation := range observations {
		if observation.Entries >= 1 && observation.FrameBytes > 8 && observation.Duration >= 0 {
			observedProposal = true
			break
		}
	}
	if !observedProposal {
		t.Fatalf("successful WAL sync observations = %#v, want a proposal frame", observations)
	}

	observationsBeforeFailure := len(observations)
	if err := nodes[1].Propose([]byte("sync-failure")); err != nil {
		t.Fatal(err)
	}
	originalOps := filePersistenceOps
	t.Cleanup(func() { filePersistenceOps = originalOps })
	filePersistenceOps.sync = func(file *os.File) error {
		if filepath.Base(file.Name()) == walName {
			return errors.New("injected WAL sync failure")
		}
		return originalOps.sync(file)
	}
	_, err := nodes[1].ProcessReady()
	filePersistenceOps = originalOps
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("ProcessReady() error = %v, want ErrFailed", err)
	}
	if len(observations) != observationsBeforeFailure {
		t.Fatalf("observer calls after failed fsync = %d, want %d", len(observations), observationsBeforeFailure)
	}
}
