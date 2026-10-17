package tcp

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"narumq/internal/adapters/memory/deliveryqueue"
	memoryretained "narumq/internal/adapters/memory/retained"
	"narumq/internal/application/membership"
	applicationdeliveryqueue "narumq/internal/application/ports/deliveryqueue"
	"narumq/internal/application/ports/deliverystate"
	"narumq/internal/application/publish"
	"narumq/internal/application/subscribe"
	"narumq/internal/domain/mqtt"
	"narumq/internal/domain/routing"
	"narumq/internal/domain/tenancy"
)

type durableTCPCommitter struct {
	changes    []membership.DeliveryChangeDTO
	err        error
	barriers   int
	barrierErr error
}

func (committer *durableTCPCommitter) CommitDelivery(_ context.Context, change membership.DeliveryChangeDTO) (membership.ApplyResultDTO, error) {
	committer.changes = append(committer.changes, change)
	if committer.err != nil {
		return membership.ApplyResultDTO{}, committer.err
	}
	return membership.ApplyResultDTO{
		Outcome:  membership.ApplyOutcomeApplied,
		Delivery: &membership.DeliveryResultDTO{Found: true},
	}, nil
}

type testPendingDelivery struct {
	result membership.ApplyResultDTO
}

func (pending testPendingDelivery) Wait() (membership.ApplyResultDTO, error) {
	return pending.result, nil
}

func (committer *durableTCPCommitter) BeginDelivery(ctx context.Context, change membership.DeliveryChangeDTO) (deliverystate.Pending, error) {
	result, err := committer.CommitDelivery(ctx, change)
	if err != nil {
		return nil, err
	}
	return testPendingDelivery{result: result}, nil
}

type testQuorumBarrier struct {
	err error
}

func (barrier testQuorumBarrier) Wait() error {
	return barrier.err
}

func (committer *durableTCPCommitter) BeginQuorumBarrier(context.Context) (deliverystate.QuorumBarrier, error) {
	committer.barriers++
	return testQuorumBarrier{err: committer.barrierErr}, nil
}

func durableTCPConnection(t *testing.T, runtime *adapterTestRuntime, version mqtt.ProtocolVersion, committer deliverystate.PublishCommitter) *connection {
	t.Helper()
	retained, err := memoryretained.New(32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	routes := routing.NewTable()
	tenantID, err := tenancy.NewTenantID("tenant")
	if err != nil {
		t.Fatal(err)
	}
	key, err := tenancy.NewSessionKey(tenantID, "publisher")
	if err != nil {
		t.Fatal(err)
	}
	sessionID := routing.SessionID(key.RoutingID())
	if err := routes.BindSessionTenant(sessionID, tenantID); err != nil {
		t.Fatal(err)
	}
	subscriberKey, err := tenancy.NewSessionKey(tenantID, "durable-subscriber")
	if err != nil {
		t.Fatal(err)
	}
	subscriberID := routing.SessionID(subscriberKey.RoutingID())
	if err := routes.BindSessionTenant(subscriberID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.Subscribe(subscriberID, mqtt.Subscription{Filter: "durable/item", QoS: mqtt.QoSAtLeastOnce}); err != nil {
		t.Fatal(err)
	}
	publications, err := publish.New(runtime.server.config.Clock, routes, runtime.server.deliveries, retained,
		publish.WithDurableDeliveryCommitter(committer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.server.publications = publications
	serverSocket, clientSocket := net.Pipe()
	t.Cleanup(func() { _ = serverSocket.Close(); _ = clientSocket.Close() })
	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = sessionID
	conn.version = version
	conn.ownership = subscribe.SessionOwnership{TenantID: "tenant", ClientID: "publisher", OwnerNodeID: 4, Epoch: 7}
	return conn
}

func durableTCPRuntime(t *testing.T, version mqtt.ProtocolVersion, committer deliverystate.PublishCommitter) (*adapterTestRuntime, *connection) {
	t.Helper()
	config := defaultAdapterConfig()
	config.ClusterMode = true
	config.LocalMemberID = 4
	config.ClusterReady = func() bool { return true }
	config.ClusterAdmissionOpen = func() bool { return true }
	config.ClusterProjectionReady = func() bool { return true }
	var conn *connection
	runtime := newAdapterTestRuntime(t, 8, config, func(server *Server) {
		conn = durableTCPConnection(t, &adapterTestRuntime{server: server}, version, committer)
	})
	t.Cleanup(func() { runtime.stop(t) })
	return runtime, conn
}

func requireControlPacket(t *testing.T, conn *connection, want mqtt.Packet) mqtt.Packet {
	t.Helper()
	select {
	case message := <-conn.controlQueue:
		got := message.packet
		switch expected := want.(type) {
		case *mqtt.PubAck:
			actual, ok := got.(*mqtt.PubAck)
			if !ok || actual.PacketID != expected.PacketID || actual.ReasonCode != expected.ReasonCode {
				t.Fatalf("control packet = %#v, want PUBACK %#v", got, expected)
			}
		case *mqtt.PubRec:
			actual, ok := got.(*mqtt.PubRec)
			if !ok || actual.PacketID != expected.PacketID || actual.ReasonCode != expected.ReasonCode {
				t.Fatalf("control packet = %#v, want PUBREC %#v", got, expected)
			}
		case *mqtt.PubComp:
			actual, ok := got.(*mqtt.PubComp)
			if !ok || actual.PacketID != expected.PacketID || actual.ReasonCode != expected.ReasonCode {
				t.Fatalf("control packet = %#v, want PUBCOMP %#v", got, expected)
			}
		default:
			t.Fatalf("unsupported expected packet %T", want)
		}
		return got
	default:
		t.Fatal("no success/control packet queued")
		return nil
	}
}

// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestTCPDurableQoS1AcknowledgesOnlyAfterCommit(t *testing.T) {
	commitErr := errors.New("replication unavailable")
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "commit succeeds"},
		{name: "commit fails", err: commitErr},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			committer := &durableTCPCommitter{err: testCase.err}
			_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
			packet := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "durable/item", Payload: []byte("v"), QoS: mqtt.QoSAtLeastOnce, PacketID: 19}
			err := conn.dispatchPublish(packet)
			if testCase.err != nil {
				if !errors.Is(err, testCase.err) {
					t.Fatalf("dispatchPublish error = %v, want %v", err, testCase.err)
				}
				select {
				case message := <-conn.controlQueue:
					t.Fatalf("failed commit emitted %T", message.packet)
				default:
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(committer.changes) != 1 || committer.changes[0].Action != membership.DeliveryActionPublish || committer.changes[0].PublishQoS != 1 {
				t.Fatalf("committed transitions = %#v, want one QoS 1 PUBLISH", committer.changes)
			}
			requireControlPacket(t, conn, &mqtt.PubAck{PacketID: 19})
		})
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-072
func TestTCPClusterDurablePublishUsesCommitWithoutReadinessPreflight(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		qos         mqtt.QoS
		packetID    mqtt.PacketIdentifier
		action      membership.DeliveryAction
		acknowledge mqtt.Packet
	}{
		{
			name: "qos1", qos: mqtt.QoSAtLeastOnce, packetID: 25,
			action: membership.DeliveryActionPublish, acknowledge: &mqtt.PubAck{PacketID: 25},
		},
		{
			name: "qos2", qos: mqtt.QoSExactlyOnce, packetID: 26,
			action: membership.DeliveryActionReceive, acknowledge: &mqtt.PubRec{PacketID: 26},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			committer := &durableTCPCommitter{}
			runtime, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
			runtime.server.config.ClusterReady = func() bool {
				t.Error("durable PUBLISH performed a separate quorum readiness check")
				return false
			}
			packet := mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "durable/item", Payload: []byte("committed"),
				QoS: testCase.qos, PacketID: testCase.packetID,
			}
			if err := conn.dispatchPublish(packet); err != nil {
				t.Fatalf("dispatchPublish: %v", err)
			}
			if len(committer.changes) != 1 || committer.changes[0].Action != testCase.action {
				t.Fatalf("committed transitions = %#v, want action %d", committer.changes, testCase.action)
			}
			requireControlPacket(t, conn, testCase.acknowledge)
		})
	}
}

func TestTCPDurableQoS2ReceiveReleaseAndCapacity(t *testing.T) {
	t.Run("commit RECEIVE before PUBREC and RELEASE before PUBCOMP", func(t *testing.T) {
		committer := &durableTCPCommitter{}
		_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
		packet := mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "durable/item", Payload: []byte("v"), QoS: mqtt.QoSExactlyOnce, PacketID: 22}
		if err := conn.dispatchPublish(packet); err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 1 || committer.changes[0].Action != membership.DeliveryActionReceive {
			t.Fatalf("before PUBREC committed = %#v, want RECEIVE", committer.changes)
		}
		requireControlPacket(t, conn, &mqtt.PubRec{PacketID: 22})
		if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 22}); err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 2 || committer.changes[1].Action != membership.DeliveryActionRelease {
			t.Fatalf("before PUBCOMP committed = %#v, want RECEIVE then RELEASE", committer.changes)
		}
		requireControlPacket(t, conn, &mqtt.PubComp{PacketID: 22})
	})

	t.Run("MQTT 5 reports full on PUBREC", func(t *testing.T) {
		committer := &durableTCPCommitter{err: applicationdeliveryqueue.ErrFull}
		_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
		if err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "full", QoS: mqtt.QoSExactlyOnce, PacketID: 23}); err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 1 || committer.changes[0].Action != membership.DeliveryActionReceive {
			t.Fatalf("capacity rejection was not preceded by RECEIVE commit: %#v", committer.changes)
		}
		requireControlPacket(t, conn, &mqtt.PubRec{PacketID: 23, ReasonCode: 0x97})
		if _, exists := conn.inbound[23]; exists {
			t.Fatal("capacity-rejected RECEIVE was retained as inbound state")
		}
	})

	t.Run("MQTT 3.1.1 closes on full without PUBREC", func(t *testing.T) {
		committer := &durableTCPCommitter{err: applicationdeliveryqueue.ErrFull}
		_, conn := durableTCPRuntime(t, mqtt.ProtocolV311, committer)
		if err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV311, Topic: "full", QoS: mqtt.QoSExactlyOnce, PacketID: 24}); err != nil {
			t.Fatal(err)
		}
		if len(committer.changes) != 1 || committer.changes[0].Action != membership.DeliveryActionReceive {
			t.Fatalf("capacity rejection was not preceded by RECEIVE commit: %#v", committer.changes)
		}
		select {
		case <-conn.stop:
		default:
			t.Fatal("capacity failure did not close MQTT 3.1.1 connection")
		}
		select {
		case message := <-conn.controlQueue:
			t.Fatalf("MQTT 3.1.1 capacity failure emitted %T", message.packet)
		default:
		}
	})
}

// Traceability: PRD MQTT-02/TEST-03/TEST-04/TEST-05; docs/specs/mqtt-protocol-and-session.md §3.1; MQTT-4.3.2-1, MQTT-4.3.2-2, MQTT-4.3.3-1, MQTT-4.3.3-2.
// Scenarios: mqtt-protocol-and-session.md:MQTT-SCN-005
func TestDispatchPublishReusesCompletedPacketIdentifier(t *testing.T) {
	for _, testCase := range []struct {
		name string
		qos  mqtt.QoS
	}{
		{name: "QoS 1", qos: mqtt.QoSAtLeastOnce},
		{name: "QoS 2", qos: mqtt.QoSExactlyOnce},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			committer := &durableTCPCommitter{}
			_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)

			publish := func(payload string) {
				t.Helper()
				packet := mqtt.Publish{
					Version: mqtt.ProtocolV5, Topic: "durable/item", Payload: []byte(payload),
					QoS: testCase.qos, PacketID: 25,
				}
				if err := conn.dispatchPublish(packet); err != nil {
					t.Fatalf("dispatchPublish(%q): %v", payload, err)
				}
				if testCase.qos == mqtt.QoSAtLeastOnce {
					requireControlPacket(t, conn, &mqtt.PubAck{PacketID: packet.PacketID})
					return
				}
				requireControlPacket(t, conn, &mqtt.PubRec{PacketID: packet.PacketID})
				if payload == "first" {
					premature := packet
					premature.Payload = []byte("premature")
					if err := conn.dispatchPublish(premature); err != nil {
						t.Fatalf("repeat PUBLISH before PUBCOMP: %v", err)
					}
					requireControlPacket(t, conn, &mqtt.PubRec{PacketID: packet.PacketID})
					if len(committer.changes) != 1 {
						t.Fatalf("premature reuse committed %d changes, want only initial RECEIVE", len(committer.changes))
					}
				}
				if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: packet.PacketID}); err != nil {
					t.Fatalf("dispatchPubRel(%q): %v", payload, err)
				}
				requireControlPacket(t, conn, &mqtt.PubComp{PacketID: packet.PacketID})
			}

			publish("first")
			publish("second")

			wantActions := []membership.DeliveryAction{membership.DeliveryActionPublish, membership.DeliveryActionPublish}
			if testCase.qos == mqtt.QoSExactlyOnce {
				wantActions = []membership.DeliveryAction{
					membership.DeliveryActionReceive, membership.DeliveryActionRelease,
					membership.DeliveryActionReceive, membership.DeliveryActionRelease,
				}
			}
			if len(committer.changes) != len(wantActions) {
				t.Fatalf("committed changes = %#v, want %d protocol transitions", committer.changes, len(wantActions))
			}
			for index, action := range wantActions {
				change := committer.changes[index]
				if change.Action != action {
					t.Fatalf("committed change %d action = %d, want %d", index, change.Action, action)
				}
				if testCase.qos == mqtt.QoSAtLeastOnce && string(change.Payload) != []string{"first", "second"}[index] {
					t.Fatalf("committed QoS 1 PUBLISH payload %d = %q", index, change.Payload)
				}
				if testCase.qos == mqtt.QoSExactlyOnce && change.PacketID != 25 {
					t.Fatalf("committed change %d packet ID = %d, want 25", index, change.PacketID)
				}
				if action == membership.DeliveryActionReceive && string(change.Payload) != []string{"first", "second"}[index/2] {
					t.Fatalf("committed RECEIVE payload %d = %q", index, change.Payload)
				}
			}
		})
	}
}

func TestTCPDurableRestoredInboundRespondsToDuplicates(t *testing.T) {
	_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, &durableTCPCommitter{})
	if err := conn.restoreInbound(&membership.DeliverySessionStateDTO{Inbound: []membership.DeliveryInboundDTO{
		{PacketID: 31, Topic: "await/release", Payload: []byte("a")},
		{PacketID: 32, Topic: "released", Released: true, ReleasedIndex: 9},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := conn.dispatchPublish(mqtt.Publish{Version: mqtt.ProtocolV5, Topic: "await/release", Payload: []byte("a"), QoS: mqtt.QoSExactlyOnce, PacketID: 31, DUP: true}); err != nil {
		t.Fatal(err)
	}
	requireControlPacket(t, conn, &mqtt.PubRec{PacketID: 31})
	if err := conn.dispatchPubRel(&mqtt.PubRel{Version: mqtt.ProtocolV5, PacketID: 32}); err != nil {
		t.Fatal(err)
	}
	packet := requireControlPacket(t, conn, &mqtt.PubComp{PacketID: 32})
	if comp := packet.(*mqtt.PubComp); comp.ReasonCode != 0 {
		t.Fatalf("duplicate tombstone PUBCOMP reason = %#x, want success", comp.ReasonCode)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestTCPDurableOutboundMetadataSurvivesWirePromotion(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 4, defaultAdapterConfig())
	defer runtime.stop(t)
	queue, err := deliveryqueue.New(4)
	if err != nil {
		t.Fatal(err)
	}
	runtime.server.deliveries = queue
	if err := queue.Register("durable-subscriber"); err != nil {
		t.Fatal(err)
	}
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = "durable-subscriber"
	conn.version = mqtt.ProtocolV5
	if !runtime.server.bindSession(conn.sessionID, conn) {
		t.Fatal("failed to bind durable subscriber connection")
	}
	delivery := applicationdeliveryqueue.DeliveryDTO{
		SessionID: conn.sessionID, Topic: "durable/out", Payload: []byte("body"), QoS: mqtt.QoSAtLeastOnce,
		DurableSequence: 41, DurableEpoch: 8,
	}
	if err := runtime.server.DeliverDurable(delivery); err != nil {
		t.Fatalf("DeliverDurable: %v", err)
	}
	select {
	case <-conn.deliveryWake:
	default:
		t.Fatal("durable delivery did not wake its live owner")
	}
	writerDone := make(chan error, 1)
	go func() { writerDone <- conn.writeDeliveries() }()
	frame := readTestFrame(t, clientSocket)
	decoder, err := mqtt.NewDecoder(1024)
	if err != nil {
		t.Fatal(err)
	}
	packet, _, err := decoder.Decode(frame, mqtt.ProtocolV5)
	if err != nil {
		t.Fatal(err)
	}
	published, ok := packet.(*mqtt.Publish)
	if !ok || published.Topic != "durable/out" || string(published.Payload) != "body" || published.QoS != mqtt.QoSAtLeastOnce {
		t.Fatalf("wire packet = %#v, want durable QoS 1 PUBLISH", packet)
	}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	outbound, err := queue.ListOutbound("durable-subscriber")
	if err != nil || len(outbound) != 1 || outbound[0].DurableSequence != 41 || outbound[0].DurableEpoch != 8 {
		t.Fatalf("wire-promoted outbound = %#v, %v", outbound, err)
	}
}

// Traceability: CLU-14; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
func TestTCPDeliverDurableClosesOwnerWhenQueueIsFull(t *testing.T) {
	runtime := newAdapterTestRuntime(t, 4, defaultAdapterConfig())
	defer runtime.stop(t)
	queue, err := deliveryqueue.New(1)
	if err != nil {
		t.Fatal(err)
	}
	runtime.server.deliveries = queue
	sessionID := routing.SessionID("durable-subscriber")
	if err := queue.Register(sessionID); err != nil {
		t.Fatal(err)
	}
	serverSocket, clientSocket := net.Pipe()
	defer serverSocket.Close()
	defer clientSocket.Close()
	conn := newConnection(runtime.server, serverSocket)
	conn.sessionID = sessionID
	if !runtime.server.bindSession(sessionID, conn) {
		t.Fatal("failed to bind durable subscriber connection")
	}
	if err := runtime.server.DeliverDurable(applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "durable/invalid", QoS: mqtt.QoSAtLeastOnce, DurableEpoch: 1,
	}); !errors.Is(err, applicationdeliveryqueue.ErrUnavailable) {
		t.Fatalf("delivery without sequence error=%v, want unavailable", err)
	}
	first := applicationdeliveryqueue.DeliveryDTO{
		SessionID: sessionID, Topic: "durable/first", QoS: mqtt.QoSAtLeastOnce,
		DurableSequence: 1, DurableEpoch: 1,
	}
	if err := runtime.server.DeliverDurable(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Topic = "durable/second"
	second.DurableSequence = 2
	if err := runtime.server.DeliverDurable(second); !errors.Is(err, applicationdeliveryqueue.ErrFull) {
		t.Fatalf("full durable queue error=%v, want full", err)
	}
	select {
	case <-conn.stop:
	default:
		t.Fatal("queue saturation did not close the live owner")
	}
	queued, err := queue.ListQueued(sessionID, time.Time{})
	if err != nil || len(queued) != 1 || queued[0].DurableSequence != first.DurableSequence {
		t.Fatalf("queue after saturation=%+v err=%v", queued, err)
	}
}

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-070
func TestTCPDurableQoS1WithoutTargetAcknowledgesAfterQuorumBarrier(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		barrierErr error
		wantErr    error
	}{
		{name: "barrier succeeds"},
		{name: "barrier fails", barrierErr: membership.ErrQuorumUnavailable, wantErr: membership.ErrQuorumUnavailable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			committer := &durableTCPCommitter{barrierErr: testCase.barrierErr}
			_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
			err := conn.dispatchPublish(mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "durable/untargeted", Payload: []byte("value"),
				QoS: mqtt.QoSAtLeastOnce, PacketID: 41,
			})
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("dispatchPublish error = %v, want %v", err, testCase.wantErr)
				}
				if committer.barriers != 1 || len(committer.changes) != 0 {
					t.Fatalf("barriers=%d changes=%+v", committer.barriers, committer.changes)
				}
				select {
				case message := <-conn.controlQueue:
					t.Fatalf("failed quorum barrier emitted %T", message.packet)
				default:
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if committer.barriers != 1 || len(committer.changes) != 0 {
				t.Fatalf("barriers=%d changes=%+v", committer.barriers, committer.changes)
			}
			requireControlPacket(t, conn, &mqtt.PubAck{PacketID: 41})
		})
	}
}

type heldTCPQuorumBarrier struct {
	entered chan struct{}
	release chan struct{}
	err     error
}

func (barrier heldTCPQuorumBarrier) Wait() error {
	close(barrier.entered)
	<-barrier.release
	return barrier.err
}

type heldTCPBarrierCommitter struct {
	durableTCPCommitter
	barrier heldTCPQuorumBarrier
}

func (committer *heldTCPBarrierCommitter) BeginQuorumBarrier(context.Context) (deliverystate.QuorumBarrier, error) {
	committer.barriers++
	return committer.barrier, nil
}

// Traceability: CLU-07; docs/specs/cluster-session-and-delivery.md §4.2, §11.11.
// Scenarios: cluster-session-and-delivery.md:CLU-SCN-070
func TestTCPDurablePhasesWithoutTargetWaitForQuorumBarrier(t *testing.T) {
	for _, barrierErr := range []error{nil, membership.ErrQuorumUnavailable} {
		name := "success"
		if barrierErr != nil {
			name = "quorum lost"
		}
		t.Run(name, func(t *testing.T) {
			committer := &heldTCPBarrierCommitter{barrier: heldTCPQuorumBarrier{
				entered: make(chan struct{}), release: make(chan struct{}), err: barrierErr,
			}}
			_, conn := durableTCPRuntime(t, mqtt.ProtocolV5, committer)
			conn.phases.start(conn)
			var release sync.Once
			t.Cleanup(func() {
				release.Do(func() { close(committer.barrier.release) })
				conn.requestClose()
				conn.phases.workers.Wait()
			})
			if err := conn.dispatchPhased(&mqtt.Publish{
				Version: mqtt.ProtocolV5, Topic: "durable/untargeted", Payload: []byte("value"),
				QoS: mqtt.QoSAtLeastOnce, PacketID: 42,
			}, 32); err != nil {
				t.Fatal(err)
			}
			select {
			case <-committer.barrier.entered:
			case <-time.After(time.Second):
				t.Fatal("completion worker did not wait for the quorum barrier")
			}
			select {
			case message := <-conn.controlQueue:
				t.Fatalf("pending quorum barrier emitted %#v", message.packet)
			default:
			}
			if committer.barriers != 1 || len(committer.changes) != 0 {
				t.Fatalf("barriers=%d changes=%+v", committer.barriers, committer.changes)
			}
			release.Do(func() { close(committer.barrier.release) })
			if barrierErr != nil {
				select {
				case <-conn.stop:
				case <-time.After(time.Second):
					t.Fatal("failed quorum barrier did not close the connection")
				}
				conn.phases.workers.Wait()
				select {
				case message := <-conn.controlQueue:
					t.Fatalf("failed quorum barrier emitted %#v", message.packet)
				default:
				}
				return
			}
			packet := readPhaseControl(t, conn)
			ack, ok := packet.(*mqtt.PubAck)
			if !ok || ack.PacketID != 42 || ack.ReasonCode != 0 {
				t.Fatalf("barrier completion emitted %#v, want successful PUBACK 42", packet)
			}
		})
	}
}
