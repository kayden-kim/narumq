package tcp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"narumq/internal/application/membership"
	"narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/deliverystate"
	"narumq/internal/domain/delivery"
	"narumq/internal/domain/mqtt"
)

type heldPhaseCommitter struct {
	action   membership.DeliveryAction
	packetID uint32
	entered  chan struct{}
	release  chan struct{}
}

func (committer *heldPhaseCommitter) CommitDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error) {
	if change.Action == committer.action && change.PacketID == committer.packetID {
		committer.entered <- struct{}{}
		select {
		case <-committer.release:
		case <-ctx.Done():
			return membership.ApplyResultDTO{}, ctx.Err()
		}
	}
	return membership.ApplyResultDTO{
		Outcome:  membership.ApplyOutcomeApplied,
		Delivery: &membership.DeliveryResultDTO{Found: true},
	}, nil
}

func (committer *heldPhaseCommitter) BeginDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (deliverystate.Pending, error) {
	result, err := committer.CommitDelivery(ctx, change)
	if err != nil {
		return nil, err
	}
	return testPendingDelivery{result: result}, nil
}

func (*heldPhaseCommitter) BeginQuorumBarrier(context.Context) (deliverystate.QuorumBarrier, error) {
	return testQuorumBarrier{}, nil
}

func readPhaseControl(t *testing.T, conn *connection) mqtt.Packet {
	t.Helper()
	select {
	case message := <-conn.controlQueue:
		return message.packet
	case <-time.After(time.Second):
		t.Fatal("handshake control packet did not arrive")
		return nil
	}
}

// Traceability: CLU-14; docs/specs/mqtt-protocol-and-session.md §3.1.
func TestTCPDurablePhasesCompleteIndependentExchangeDuringReceiveCommit(t *testing.T) {
	committer := &heldPhaseCommitter{
		action: membership.DeliveryActionReceive, packetID: 2,
		entered: make(chan struct{}, 1), release: make(chan struct{}),
	}
	_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
	first := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "durable/item", Payload: []byte("first"), QoS: mqtt.QoSExactlyOnce, PacketID: 1}
	if err := conn.dispatchPublish(first); err != nil {
		t.Fatal(err)
	}
	requireControlPacket(t, conn, &mqtt.PubRec{PacketID: 1})
	conn.phases.start(conn)
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(committer.release) })
		conn.requestClose()
		conn.phases.workers.Wait()
	})
	second := first
	second.PacketID, second.Payload = 2, []byte("second")
	if err := conn.dispatchPhased(&second, 32); err != nil {
		t.Fatal(err)
	}
	select {
	case <-committer.entered:
	case <-time.After(time.Second):
		t.Fatal("second RECEIVE never entered durable commit")
	}
	if err := conn.dispatchPhased(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 1}, 4); err != nil {
		t.Fatal(err)
	}
	packet := readPhaseControl(t, conn)
	comp, ok := packet.(*mqtt.PubComp)
	if !ok || comp.PacketID != 1 || comp.ReasonCode != 0 {
		t.Fatalf("control while RECEIVE 2 is held = %#v, want successful PUBCOMP 1", packet)
	}
	conn.deliveryMu.Lock()
	firstComplete := conn.inbound[1].state.IsComplete()
	_, secondAccepted := conn.inbound[2]
	conn.deliveryMu.Unlock()
	if !firstComplete || secondAccepted {
		t.Fatalf("held-commit state: first complete=%v, second accepted=%v", firstComplete, secondAccepted)
	}
	releaseOnce.Do(func() { close(committer.release) })
	packet = readPhaseControl(t, conn)
	rec, ok := packet.(*mqtt.PubRec)
	if !ok || rec.PacketID != 2 || rec.ReasonCode != 0 {
		t.Fatalf("control after RECEIVE 2 commits = %#v, want successful PUBREC 2", packet)
	}
	if err := conn.dispatchPhased(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 2}, 4); err != nil {
		t.Fatal(err)
	}
	packet = readPhaseControl(t, conn)
	comp, ok = packet.(*mqtt.PubComp)
	if !ok || comp.PacketID != 2 || comp.ReasonCode != 0 {
		t.Fatalf("second exchange completion = %#v, want successful PUBCOMP 2", packet)
	}
}

// Traceability: CLU-14; docs/specs/mqtt-protocol-and-session.md §3.1.
func TestTCPDurablePhasesPreserveDuplicateReleaseBeforeIdentifierReuse(t *testing.T) {
	committer := &heldPhaseCommitter{
		action: membership.DeliveryActionRelease, packetID: 7,
		entered: make(chan struct{}, 1), release: make(chan struct{}),
	}
	_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
	publish := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "durable/item", Payload: []byte("first"), QoS: mqtt.QoSExactlyOnce, PacketID: 7}
	if err := conn.dispatchPublish(publish); err != nil {
		t.Fatal(err)
	}
	requireControlPacket(t, conn, &mqtt.PubRec{PacketID: 7})
	conn.phases.start(conn)
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(committer.release) })
		conn.requestClose()
		conn.phases.workers.Wait()
	})
	rel := &mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 7}
	if err := conn.dispatchPhased(rel, 4); err != nil {
		t.Fatal(err)
	}
	select {
	case <-committer.entered:
	case <-time.After(time.Second):
		t.Fatal("RELEASE never entered durable commit")
	}
	if err := conn.dispatchPhased(rel, 4); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(committer.release) })
	packet := readPhaseControl(t, conn)
	if comp, ok := packet.(*mqtt.PubComp); !ok || comp.PacketID != 7 {
		t.Fatalf("initial completion = %#v, want PUBCOMP 7", packet)
	}
	publish.Payload = []byte("reused")
	if err := conn.dispatchPhased(&publish, 32); err != nil {
		t.Fatal(err)
	}
	packet = readPhaseControl(t, conn)
	if comp, ok := packet.(*mqtt.PubComp); !ok || comp.PacketID != 7 {
		t.Fatalf("duplicate RELEASE = %#v, want PUBCOMP 7 before new PUBREC", packet)
	}
	packet = readPhaseControl(t, conn)
	if rec, ok := packet.(*mqtt.PubRec); !ok || rec.PacketID != 7 {
		t.Fatalf("new exchange = %#v, want PUBREC 7", packet)
	}
	conn.deliveryMu.Lock()
	complete := conn.inbound[7].state.IsComplete()
	conn.deliveryMu.Unlock()
	if complete {
		t.Fatal("duplicate RELEASE completed the reused identifier's new exchange")
	}
}

type heldPromotionQueue struct {
	deliveryqueue.Queue
	entered chan deliveryqueue.OutboundDTO
	release chan struct{}
}

func (queue *heldPromotionQueue) PromoteHead(outbound deliveryqueue.OutboundDTO, now time.Time) error {
	queue.entered <- outbound
	<-queue.release
	return queue.Queue.PromoteHead(outbound, now)
}

func (queue *heldPromotionQueue) PromoteBatch(outbounds []deliveryqueue.OutboundDTO, now time.Time) (int, error) {
	if len(outbounds) == 0 {
		return 0, nil
	}
	err := queue.PromoteHead(outbounds[0], now)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// Traceability: MQTT-02/CLU-14; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestWriteDeliveriesReservesPacketIDWhilePromotionWaits(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 8, defaultAdapterConfig())
	defer runtime.stop(t)
	serverSocket, clientSocket := net.Pipe()
	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID, conn.version = "reservation-client", mqtt.ProtocolV5
	t.Cleanup(func() { conn.requestClose(); _ = clientSocket.Close() })
	if err := runtime.server.deliveries.Register(conn.sessionID); err != nil {
		t.Fatal(err)
	}
	enqueue := func(topic string) {
		t.Helper()
		if err := runtime.server.deliveries.TryEnqueue(deliveryqueue.DeliveryDTO{
			SessionID: conn.sessionID, Topic: topic, Payload: []byte(topic), QoS: mqtt.QoSAtLeastOnce,
		}, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	enqueue("first")
	firstDone := make(chan error, 1)
	go func() { firstDone <- conn.writeDeliveries() }()
	frame := readTestFrame(t, clientSocket)
	packet, _, err := runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := packet.(*mqtt.Publish)
	if !ok {
		t.Fatalf("first wire packet = %T, want PUBLISH", packet)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	held := &heldPromotionQueue{Queue: runtime.server.deliveries, entered: make(chan deliveryqueue.OutboundDTO, 1), release: make(chan struct{})}
	runtime.server.deliveries = held
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(held.release) }) })
	enqueue("second")
	secondDone := make(chan error, 1)
	go func() { secondDone <- conn.writeDeliveries() }()
	var pending deliveryqueue.OutboundDTO
	select {
	case pending = <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("second promotion never entered I/O")
	}
	ackDone := make(chan error, 1)
	go func() {
		ackDone <- conn.dispatchPubAck(&mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: first.PacketID})
	}()
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("prior PUBACK was blocked by the next promotion's I/O")
	}
	conn.deliveryMu.Lock()
	conn.packetIDs.next = uint16(pending.PacketID)
	allocated, allocationErr := conn.packetIDs.acquire()
	conn.packetIDs.release(allocated)
	conn.deliveryMu.Unlock()
	if allocationErr != nil || allocated == pending.PacketID {
		t.Fatalf("allocation during pending promotion = %d, %v; reserved ID %d must remain unavailable", allocated, allocationErr, pending.PacketID)
	}
	releaseOnce.Do(func() { close(held.release) })
	frame = readTestFrame(t, clientSocket)
	packet, _, err = runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	second, ok := packet.(*mqtt.Publish)
	if !ok || second.PacketID != pending.PacketID || second.Topic != "second" {
		t.Fatalf("committed promotion wire packet = %#v, want second PUBLISH using reserved ID %d", packet, pending.PacketID)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

type submittedPhaseCommitter struct {
	submitted chan int
	release   [2]chan struct{}
	failures  [2]error
	next      int
}

type heldDeliveryResult struct {
	release <-chan struct{}
	err     error
}

func (pending heldDeliveryResult) Wait() (membership.ApplyResultDTO, error) {
	<-pending.release
	if pending.err != nil {
		return membership.ApplyResultDTO{}, pending.err
	}
	return membership.ApplyResultDTO{
		Outcome:  membership.ApplyOutcomeApplied,
		Delivery: &membership.DeliveryResultDTO{Found: true},
	}, nil
}

func (committer *submittedPhaseCommitter) BeginDelivery(_ context.Context, change membership.DeliveryChangeDTO) (deliverystate.Pending, error) {
	if change.Action != membership.DeliveryActionPublish && change.Action != membership.DeliveryActionReceive {
		return testPendingDelivery{result: membership.ApplyResultDTO{
			Outcome:  membership.ApplyOutcomeApplied,
			Delivery: &membership.DeliveryResultDTO{Found: true},
		}}, nil
	}
	index := committer.next
	committer.next++
	committer.submitted <- index
	return heldDeliveryResult{release: committer.release[index], err: committer.failures[index]}, nil
}

func (committer *submittedPhaseCommitter) CommitDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error) {
	pending, err := committer.BeginDelivery(ctx, change)
	if err != nil {
		return membership.ApplyResultDTO{}, err
	}
	return pending.Wait()
}

func (*submittedPhaseCommitter) BeginQuorumBarrier(context.Context) (deliverystate.QuorumBarrier, error) {
	return testQuorumBarrier{}, nil
}

// Traceability: CLU-14; docs/specs/mqtt-protocol-and-session.md §3.1.
func TestTCPDurablePublishSubmitsBeforePriorCommitCompletes(t *testing.T) {
	for _, qos := range []mqtt.QoS{mqtt.QoSAtLeastOnce, mqtt.QoSExactlyOnce} {
		t.Run(fmt.Sprintf("qos-%d", qos), func(t *testing.T) {
			committer := &submittedPhaseCommitter{
				submitted: make(chan int, 2),
				release:   [2]chan struct{}{make(chan struct{}), make(chan struct{})},
			}
			_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
			conn.phases.start(conn)
			var release [2]sync.Once
			t.Cleanup(func() {
				for i := range release {
					release[i].Do(func() { close(committer.release[i]) })
				}
				conn.requestClose()
				conn.phases.workers.Wait()
			})
			for i := range 2 {
				packet := &mqtt.Publish{
					Version: mqtt.ProtocolV5, Topic: "durable/item",
					Payload: []byte{byte(i)}, QoS: qos, PacketID: mqtt.PacketIdentifier(i + 1),
				}
				if err := conn.dispatchPhased(packet, 32); err != nil {
					t.Fatal(err)
				}
				select {
				case index := <-committer.submitted:
					if index != i {
						t.Fatalf("submitted exchange = %d, want %d", index, i)
					}
				case <-time.After(time.Second):
					t.Fatalf("PUBLISH %d was not submitted while the preceding commit was pending", i+1)
				}
			}
			// Completing the later result must not acknowledge either exchange
			// while the first result remains uncommitted.
			release[1].Do(func() { close(committer.release[1]) })
			select {
			case message := <-conn.controlQueue:
				t.Fatalf("premature or reordered acknowledgment: %#v", message.packet)
			default:
			}
			release[0].Do(func() { close(committer.release[0]) })
			for i := range 2 {
				packet := readPhaseControl(t, conn)
				if qos == mqtt.QoSAtLeastOnce {
					ack, ok := packet.(*mqtt.PubAck)
					if !ok || ack.PacketID != mqtt.PacketIdentifier(i+1) || ack.ReasonCode != 0 {
						t.Fatalf("acknowledgment %d = %#v, want successful PUBACK %d", i, packet, i+1)
					}
				} else {
					rec, ok := packet.(*mqtt.PubRec)
					if !ok || rec.PacketID != mqtt.PacketIdentifier(i+1) || rec.ReasonCode != 0 {
						t.Fatalf("acknowledgment %d = %#v, want successful PUBREC %d", i, packet, i+1)
					}
				}
			}
		})
	}
}

// Traceability: CLU-14; docs/specs/mqtt-protocol-and-session.md §3.1.
func TestTCPDurablePublishFailureDrainsPendingWithoutSuccessAck(t *testing.T) {
	for _, qos := range []mqtt.QoS{mqtt.QoSAtLeastOnce, mqtt.QoSExactlyOnce} {
		t.Run(fmt.Sprintf("qos-%d", qos), func(t *testing.T) {
			committer := &submittedPhaseCommitter{
				submitted: make(chan int, 2),
				release:   [2]chan struct{}{make(chan struct{}), make(chan struct{})},
				failures:  [2]error{membership.ErrQuorumUnavailable, nil},
			}
			_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
			conn.phases.start(conn)
			var release [2]sync.Once
			t.Cleanup(func() {
				for i := range release {
					release[i].Do(func() { close(committer.release[i]) })
				}
				conn.requestClose()
				conn.phases.workers.Wait()
			})
			for i := range 2 {
				packet := &mqtt.Publish{
					Version: mqtt.ProtocolV5, Topic: "durable/item", Payload: []byte{byte(i)},
					QoS: qos, PacketID: mqtt.PacketIdentifier(i + 1),
				}
				if err := conn.dispatchPhased(packet, 32); err != nil {
					t.Fatal(err)
				}
				select {
				case <-committer.submitted:
				case <-time.After(time.Second):
					t.Fatal("independent PUBLISH did not reach submission")
				}
			}
			release[1].Do(func() { close(committer.release[1]) })
			release[0].Do(func() { close(committer.release[0]) })
			select {
			case <-conn.stop:
			case <-time.After(time.Second):
				t.Fatal("failed pending commit did not close the connection")
			}
			conn.phases.workers.Wait()
			select {
			case message := <-conn.controlQueue:
				t.Fatalf("success acknowledgment after pending commit failure: %#v", message.packet)
			default:
			}
			conn.deliveryMu.Lock()
			accepted := len(conn.inbound)
			conn.deliveryMu.Unlock()
			if accepted != 0 {
				t.Fatalf("accepted inbound exchanges after failed commit and shutdown = %d", accepted)
			}
		})
	}
}

type heldOutboundQueue struct {
	deliveryqueue.Queue
	submitted chan mqtt.PacketIdentifier
	release   [2]chan struct{}
}

type heldOutboundTransition struct {
	queue    *heldOutboundQueue
	outbound deliveryqueue.OutboundDTO
	complete bool
}

func (pending heldOutboundTransition) Wait() error {
	<-pending.queue.release[pending.outbound.PacketID-1]
	if pending.complete {
		return pending.queue.Queue.CompleteOutbound(pending.outbound.SessionID, pending.outbound.PacketID)
	}
	return pending.queue.Queue.ReplaceOutbound(pending.outbound)
}

func (queue *heldOutboundQueue) BeginCompleteOutbound(outbound deliveryqueue.OutboundDTO) (deliveryqueue.PendingTransition, error) {
	queue.submitted <- outbound.PacketID
	return heldOutboundTransition{queue: queue, outbound: outbound, complete: true}, nil
}

func (queue *heldOutboundQueue) BeginReplaceOutbound(outbound deliveryqueue.OutboundDTO) (deliveryqueue.PendingTransition, error) {
	queue.submitted <- outbound.PacketID
	return heldOutboundTransition{queue: queue, outbound: outbound}, nil
}

// Traceability: CLU-14; docs/specs/mqtt-protocol-and-session.md §3.1.
func TestTCPDurableOutboundAckKeepsIdentifierUntilOrderedCommit(t *testing.T) {
	for _, qos := range []mqtt.QoS{mqtt.QoSAtLeastOnce, mqtt.QoSExactlyOnce} {
		t.Run(fmt.Sprintf("qos-%d", qos), func(t *testing.T) {
			runtime, conn := durableTCPRuntime(t, mqtt.ProtocolV5, &durableTCPCommitter{})
			queue := &heldOutboundQueue{
				Queue: runtime.server.deliveries, submitted: make(chan mqtt.PacketIdentifier, 2),
				release: [2]chan struct{}{make(chan struct{}), make(chan struct{})},
			}
			runtime.server.deliveries = queue
			if err := queue.Register(conn.sessionID); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				packetID, err := conn.packetIDs.acquire()
				if err != nil {
					t.Fatal(err)
				}
				packet := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "durable/item", QoS: qos, PacketID: packetID}
				state, _, err := delivery.NewOutbound(packet)
				if err != nil {
					t.Fatal(err)
				}
				outbound := deliveryqueue.OutboundDTO{SessionID: conn.sessionID, PacketID: packetID, State: state}
				if err := queue.TryEnqueue(deliveryqueue.DeliveryDTO{SessionID: conn.sessionID, Topic: packet.Topic, QoS: qos}, time.Time{}); err != nil {
					t.Fatal(err)
				}
				if err := queue.PromoteHead(outbound, time.Time{}); err != nil {
					t.Fatal(err)
				}
				conn.outbound[packetID] = outbound
			}
			conn.phases.start(conn)
			var release [2]sync.Once
			t.Cleanup(func() {
				for i := range release {
					release[i].Do(func() { close(queue.release[i]) })
				}
				conn.requestClose()
				conn.phases.workers.Wait()
			})
			for i := range 2 {
				packetID := mqtt.PacketIdentifier(i + 1)
				var packet mqtt.Packet = &mqtt.PubAck{Version: mqtt.ProtocolV5, PacketID: packetID}
				if qos == mqtt.QoSExactlyOnce {
					packet = &mqtt.PubRec{Version: mqtt.ProtocolV5, PacketID: packetID}
				}
				if err := conn.dispatchPhased(packet, 4); err != nil {
					t.Fatal(err)
				}
				select {
				case id := <-queue.submitted:
					if id != packetID {
						t.Fatalf("outbound submission order = %d, want %d", id, packetID)
					}
				case <-time.After(time.Second):
					t.Fatal("independent ACK waited for the preceding commit")
				}
			}
			conn.deliveryMu.Lock()
			conn.packetIDs.next = 1
			reserved, err := conn.packetIDs.acquire()
			conn.packetIDs.release(reserved)
			conn.deliveryMu.Unlock()
			if err != nil || reserved != 3 {
				t.Fatalf("allocator reused a commit-pending ID: id=%d err=%v", reserved, err)
			}
			select {
			case message := <-conn.controlQueue:
				t.Fatalf("wire transition before commit: %#v", message.packet)
			default:
			}
			for i := range release {
				release[i].Do(func() { close(queue.release[i]) })
			}
			if err := conn.phases.flush(conn); err != nil {
				t.Fatal(err)
			}
			if qos == mqtt.QoSExactlyOnce {
				for i := range 2 {
					packet := readPhaseControl(t, conn)
					rel, ok := packet.(*mqtt.PubRel)
					if !ok || rel.PacketID != mqtt.PacketIdentifier(i+1) {
						t.Fatalf("ordered commit wire result = %#v, want PUBREL %d", packet, i+1)
					}
				}
			} else {
				conn.deliveryMu.Lock()
				conn.packetIDs.next = 1
				id, err := conn.packetIDs.acquire()
				conn.packetIDs.release(id)
				conn.deliveryMu.Unlock()
				if err != nil || id != 1 {
					t.Fatalf("completed ID remains reserved: id=%d err=%v", id, err)
				}
			}
		})
	}
}

type partialPromotionQueue struct {
	deliveryqueue.Queue
	failOnce bool
}

func (queue *partialPromotionQueue) PromoteBatch(outbounds []deliveryqueue.OutboundDTO, now time.Time) (int, error) {
	if queue.failOnce {
		queue.failOnce = false
		count, err := queue.Queue.PromoteBatch(outbounds[:1], now)
		if err != nil {
			return count, err
		}
		return count, deliveryqueue.ErrRetryableUnavailable
	}
	return queue.Queue.PromoteBatch(outbounds, now)
}

// Traceability: CLU-14; docs/specs/mqtt-protocol-and-session.md §3.2.
func TestTCPPartialPromotionPreservesEverySuffixIdentifier(t *testing.T) {
	for _, mode := range []string{"durable", "local"} {
		t.Run(mode, func(t *testing.T) {
			runtime := newAdapterTestRuntime(t, 8, defaultAdapterConfig())
			defer runtime.stop(t)
			queue := &partialPromotionQueue{Queue: runtime.server.deliveries, failOnce: true}
			runtime.server.deliveries = queue
			serverSocket, clientSocket := net.Pipe()
			defer serverSocket.Close()
			defer clientSocket.Close()
			conn := newConnection(runtime.server, serverSocket)
			conn.sessionID = "partial-promotion"
			conn.version = mqtt.ProtocolV5
			if err := queue.Register(conn.sessionID); err != nil {
				t.Fatal(err)
			}
			now := runtime.server.config.Clock.Now()
			for sequence := uint64(1); sequence <= 3; sequence++ {
				durableSequence := sequence
				if mode == "local" {
					durableSequence = 0
				}
				if err := queue.TryEnqueue(deliveryqueue.DeliveryDTO{
					SessionID: conn.sessionID, Topic: fmt.Sprintf("events/%d", sequence), QoS: mqtt.QoSAtLeastOnce,
					DurableSequence: durableSequence, DurableEpoch: 1,
				}, now); err != nil {
					t.Fatal(err)
				}
			}
			result := make(chan error, 1)
			go func() { result <- conn.writeDeliveries() }()
			checkPublish := func(sequence uint64) {
				t.Helper()
				frame := readTestFrame(t, clientSocket)
				packet, _, err := runtime.server.limits.Load().decoder.Decode(frame, mqtt.ProtocolV5)
				publish, ok := packet.(*mqtt.Publish)
				if err != nil || !ok || publish.Topic != fmt.Sprintf("events/%d", sequence) || publish.PacketID != mqtt.PacketIdentifier(sequence) {
					t.Fatalf("delivery %d: packet=%#v err=%v", sequence, packet, err)
				}
			}
			checkPublish(1)
			if err := <-result; err != deliveryqueue.ErrRetryableUnavailable {
				t.Fatalf("partial promotion error=%v", err)
			}
			conn.deliveryMu.Lock()
			conn.packetIDs.next = 1
			id, err := conn.packetIDs.acquire()
			conn.packetIDs.release(id)
			conn.deliveryMu.Unlock()
			if err != nil || id != 4 {
				t.Fatalf("suffix identifier was released before retry: acquired=%d err=%v", id, err)
			}
			go func() { result <- conn.writeDeliveries() }()
			checkPublish(2)
			checkPublish(3)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			outbounds, err := queue.ListOutbound(conn.sessionID)
			if err != nil || len(outbounds) != 3 {
				t.Fatalf("outbound retry state=%+v err=%v", outbounds, err)
			}
			for _, outbound := range outbounds {
				expectedSequence := uint64(outbound.PacketID)
				if mode == "local" {
					expectedSequence = 0
				}
				if outbound.DurableSequence != expectedSequence {
					t.Fatalf("retry changed delivery/identifier pairing: %+v", outbound)
				}
			}
		})
	}
}
